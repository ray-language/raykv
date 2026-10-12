# raykv

Servidor clave-valor **hablando RESP2**, escrito en [raylang](https://github.com/ray-language/raylang): `redis-cli`, `redis-benchmark` y el cliente `net/redis` le hablan sin saber que no es Redis. Subconjunto: `PING ECHO SET(EX/PX/NX/XX) GET DEL EXISTS INCR/DECR/INCRBY EXPIRE/PEXPIRE/TTL/PTTL TYPE KEYS DBSIZE FLUSHALL SUBSCRIBE/UNSUBSCRIBE/PUBLISH`, con expiración (perezosa + barrido periódico) y persistencia AOF con rewrite al arrancar.

```text
$ raykv &
raykv listening on port 7379 (aof on, fsync data)

$ redis-cli -p 7379 SET saludo hola
OK
$ redis-cli -p 7379 SET fugaz x EX 1
OK
$ redis-cli -p 7379 SUBSCRIBE noticias   # y en otra terminal: PUBLISH noticias "hola"
```

## El benchmark honesto (contra Redis real)

`redis-benchmark -t set,get -n 50000 -c 50`, misma máquina (Apple M3 Pro, 11 núcleos),
re-medido con raylang 1.27.46 (octubre 2026; mediana de 3–5 corridas):

| Servidor | SET | GET | p50 SET |
|---|---|---|---|
| **raykv nativo (AOF off)** | 155.3k rps | 155.8k rps | 0.21 ms |
| raykv VM (AOF off) | 74.5k rps | 74.9k rps | 0.67 ms |
| Redis 8.8 real (sin persistencia) | 174.8k rps | 176.1k rps | 0.16 ms |
| **raykv nativo (AOF, `--fsync data`: `sync_data` por tanda)** | 137.7k rps | 156.3k rps | 0.26 ms |
| raykv nativo (AOF, `--fsync full`: `sync` por tanda) | 5.6k rps | — | 8.6 ms |
| Redis 8.8 real (`appendfsync always`) | 5.5k rps | 162.3k rps | 9.1 ms |
| raykv nativo (AOF, `sync_data` por append, hasta este cambio) | 38.4k rps | — | 1.24 ms |
| raykv nativo (AOF, `sync` por append, ≤ 1.27.20) | ~240 rps | — | 189 ms |

**raykv nativo queda a ~88% de Redis real** con 50 conexiones concurrentes —
el camino socket → parser RESP incremental → actor → Map → respuesta aguanta.
La VM queda a ~43% de Redis.

**La AOF hace *group commit*.** Ninguna respuesta sale antes de que su
escritura esté en disco (la política `appendfsync always` de Redis), pero el
actor no vuelca escritura por escritura. Toma un mensaje, vacía lo que ya está
en cola detrás (hasta 256), hace **un** volcado y solo entonces responde.
Bajo carga la cola se llena mientras dura el volcado, y uno solo cubre muchas
escrituras. Redis hace lo mismo en cada vuelta de su bucle de eventos.

- `--fsync data` (por defecto): `fs.sync_data` (fdatasync) por tanda.
  ~138k SET/s, 3,6× el volcado por append y ~89% de raykv sin AOF. Sobrevive
  a un crash del proceso o del SO, pero en APFS no vacía la caché del disco:
  un corte de luz puede perder la última tanda.
- `--fsync full`: `fs.sync` (`F_FULLFSYNC` en macOS, ~4 ms) por tanda.
  Sobrevive también a un corte de luz. ~5,6k SET/s con 50 clientes, lo mismo
  que Redis con `appendfsync always`, que en macOS también usa `F_FULLFSYNC`
  (con un solo cliente, los dos rondan las ~200 escrituras/s).

El agrupado solo rinde con concurrencia: con 1 conexión no hay cola, y
`--fsync data` hace ~19k SET/s igual que antes. El rewrite del arranque pasa
siempre por `fs.sync` antes del rename.

## Cómo está hecho

- **Parser RESP2 incremental y binario-seguro** (`src/resp.ray`): consume de
  un buffer creciente y devuelve un comando por llamada o `NeedMore` con el
  frame partido por TCP; comandos inline (`PING\r\n`) incluidos.
- **Un actor dueño del keyspace** (como Redis: single-threaded donde importa):
  cada conexión es una fibra que parsea y serializa sus comandos por canal.
  Pub/sub: el actor guarda los canales de salida de los suscriptores; una
  conexión suscrita gana una fibra escritora. `PUBLISH` entrega con
  `try_send`: un suscriptor lento (buffer de 64 mensajes lleno) pierde ese
  mensaje en vez de bloquear al actor — y con él a todos los clientes.
- Sockets de cliente con `TCP_NODELAY` y `SO_KEEPALIVE` (como Redis).
- **AOF lógica**: `INCR` se persiste como el `SET` de su resultado; TTL como
  `PEXPIREAT` absoluto en epoch-ms (sobrevive reinicios con reloj de pared).
  Replay con el mismo parser del servidor, recorriendo el archivo por
  desplazamiento (`resp.parse_at`; cola rota = descartada); rewrite
  compactante al arrancar con `fs.rename` atómico (el temporal pasa por
  `fs.sync` antes del rename). `kill -9` probado.
- Expiración: perezosa en cada acceso + barrido cada 250 ms.
- `COMMAND`/`CONFIG`/`CLIENT`/`INFO` responden benignamente (el ruido de
  handshake de redis-cli/redis-benchmark).

## Estado actual

| Capacidad | Estado |
|-----------|--------|
| RESP2 incremental (multibulk + inline), binario-seguro en valores | ✅ |
| SET/GET/DEL/EXISTS/INCR*/EXPIRE*/TTL*/TYPE/KEYS(glob)/DBSIZE/FLUSHALL | ✅ |
| Pub/sub (SUBSCRIBE/UNSUBSCRIBE/PUBLISH) con limpieza al desconectar | ✅ |
| Un suscriptor lento no bloquea el keyspace (`try_send`) | ✅ |
| AOF lógica + replay + rewrite compactante (kill -9 probado) | ✅ |
| Compatible redis-cli, redis-benchmark y `net/redis` (tests con ambos) | ✅ |
| Binario nativo a ~88% de Redis real | ✅ |
| Tests (parser incremental, actor, AOF, E2E con net/redis + reinicio) | ✅ 13 |
| Group commit: un volcado por tanda, respuesta después del volcado (`--fsync data\|full`) | ✅ |
| Valores binarios en la AOF | ✅ (raylang M115.1: `fs.write_bytes`) |
| Arranque lineal: 200k claves en ~0,5 s (antes ~38 s: replay y rewrite cuadráticos) | ✅ |
| `appendfsync everysec` (volcar una vez por segundo, respondiendo antes) | 📋 v2 |
| Hashes/listas/sets, RDB snapshot, replicación | 📋 fuera de v1 |

## Hallazgos de dogfood (necesidades confirmadas del lenguaje)

Anotados en `raylang/IDEAS.md` §68:

1. **Nativo: un `return;` dentro de una clausura `spawn` rompe el build**
   (E0308: el cierre generado infiere `()` y choca con `__RaySend::U`).
   [RESUELTO en raylang, PR #140: el cuerpo del spawn se emite como IIFE y
   `return;` compila; el bucle de accept ya usa `return` sin bandera.]
2. **[RESUELTO — raylang M115.1]** No hay `fs.write_bytes(handle, bytes)`:
   existe, y la AOF es BINARIA (valores \x00/\xff/no-UTF-8 intactos).
3. **[RESUELTO — raylang M115.1]** La ausencia de fsync: cada append pasa por
   `fs.sync`. **[RESUELTO — raylang 1.27.21]** `fs.sync_data` ya es barato en
   APFS (~36k SET/s frente a ~240): lo usa cada append.
4. **Positivo**: el pipeline bytes → parser incremental → actor → Map aguanta
   ~150k ops/s con 50 conexiones — el costo del round-trip por canal por
   comando es asumible incluso a escala redis-benchmark.

## Desarrollo

Requiere raylang ≥ 1.27.21; la dependencia `net` viene del índice de paquetes
(`net = "^0.3.3"` en `ray.toml`, versión exacta fijada en `ray.lock`).

```sh
ray test                          # 13 tests
ray run src/main.ray --no-aof --port 7379
ray run src/main.ray --fsync full --port 7379   # AOF que sobrevive a un corte de luz
ray build --native src/main.ray -o raykv --release
redis-benchmark -p 7379 -t set,get -n 50000 -c 50 -q
```

Estructura: `src/main.ray` (CLI) · `resp.ray` (parser/encoders) · `store.ray`
(actor del keyspace + pub/sub + expiry) · `aof.ray` (log + rewrite) ·
`server.ray` (accept + fibra por conexión).

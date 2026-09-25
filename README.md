# raykv

Servidor clave-valor **hablando RESP2**, escrito en [raylang](https://github.com/ray-language/raylang): `redis-cli`, `redis-benchmark` y el cliente `net/redis` le hablan sin saber que no es Redis. Subconjunto: `PING ECHO SET(EX/PX/NX/XX) GET DEL EXISTS INCR/DECR/INCRBY EXPIRE/PEXPIRE/TTL/PTTL TYPE KEYS DBSIZE FLUSHALL SUBSCRIBE/UNSUBSCRIBE/PUBLISH`, con expiración (perezosa + barrido periódico) y persistencia AOF con rewrite al arrancar.

```text
$ raykv &
raykv listening on port 7379 (aof on)

$ redis-cli -p 7379 SET saludo hola
OK
$ redis-cli -p 7379 SET fugaz x EX 1
OK
$ redis-cli -p 7379 SUBSCRIBE noticias   # y en otra terminal: PUBLISH noticias "hola"
```

## El benchmark honesto (contra Redis real)

`redis-benchmark -t set,get -n 50000 -c 50`, misma máquina (Apple Silicon),
re-medido con raylang 1.27.11 (septiembre 2026):

| Servidor | SET | GET | p50 |
|---|---|---|---|
| **raykv nativo (AOF off)** | 147.5k rps | 154.3k rps | 0.20 ms |
| raykv VM (AOF off) | 72.9k rps | 75.5k rps | 0.68 ms |
| Redis 7 real (sin persistencia) | 175.4k rps | 175.4k rps | 0.15 ms |
| raykv nativo (AOF on, fsync por append) | ~240 rps | — | 189 ms |

**raykv nativo queda a ~84–88% de Redis real** con 50 conexiones concurrentes —
el camino socket → parser RESP incremental → actor → Map → respuesta aguanta.
La AOF **con fsync por escritura** (la política `appendfsync always` de Redis)
sí cuesta: en macOS cada `fs.sync` ronda los 4 ms y el actor lo serializa, así
que las escrituras caen a ~240/s. Un modo `everysec` (fsync agrupado por un
temporizador) es el siguiente paso — ver v2. La VM queda a ~43% de Redis.

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
  Replay con el mismo parser del servidor (cola rota = descartada); rewrite
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
| Binario nativo a ~84–88% de Redis real | ✅ |
| Tests (parser incremental, actor, E2E con net/redis + reinicio) | ✅ 9 |
| fsync (durabilidad ante corte de luz) | ✅ (raylang M115.1: `fs.sync` por append) |
| Valores binarios en la AOF | ✅ (raylang M115.1: `fs.write_bytes`) |
| `appendfsync everysec` (fsync agrupado; hoy es por append) | 📋 v2 |
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
   `fs.sync` (durable ante corte de luz).
4. **Positivo**: el pipeline bytes → parser incremental → actor → Map aguanta
   ~150k ops/s con 50 conexiones — el costo del round-trip por canal por
   comando es asumible incluso a escala redis-benchmark.

## Desarrollo

Requiere raylang ≥ 1.27; la dependencia `net` viene del índice de paquetes
(`net = "^0.3.3"` en `ray.toml`, versión exacta fijada en `ray.lock`).

```sh
ray test                          # 9 tests
ray run src/main.ray --no-aof --port 7379
ray build --native src/main.ray -o raykv --release
redis-benchmark -p 7379 -t set,get -n 50000 -c 50 -q
```

Estructura: `src/main.ray` (CLI) · `resp.ray` (parser/encoders) · `store.ray`
(actor del keyspace + pub/sub + expiry) · `aof.ray` (log + rewrite) ·
`server.ray` (accept + fibra por conexión).

# raykv

Servidor clave-valor **hablando RESP2**, escrito en [raylang](https://github.com/roberto-ayala/raylang): `redis-cli`, `redis-benchmark` y el cliente `net/redis` le hablan sin saber que no es Redis. Subconjunto: `PING ECHO SET(EX/PX/NX/XX) GET DEL EXISTS INCR/DECR/INCRBY EXPIRE/PEXPIRE/TTL/PTTL TYPE KEYS DBSIZE FLUSHALL SUBSCRIBE/UNSUBSCRIBE/PUBLISH`, con expiración (perezosa + barrido periódico) y persistencia AOF con rewrite al arrancar.

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

`redis-benchmark -t set,get -n 50000 -c 50`, misma máquina (Apple Silicon):

| Servidor | SET | GET | p50 |
|---|---|---|---|
| **raykv nativo (AOF on)** | 124.1k rps | 133.0k rps | 0.23 ms |
| raykv nativo (AOF off) | 124.7k rps | 134.0k rps | 0.23 ms |
| raykv VM (AOF off) | 81.3k rps | 83.9k rps | 0.55 ms |
| Redis 7 real (sin persistencia) | 144.9k rps | 154.3k rps | 0.18 ms |

**raykv nativo queda a ~86% de Redis real** con 50 conexiones concurrentes —
el camino socket → parser RESP incremental → actor → Map → respuesta aguanta.
La AOF apenas cuesta (ahora con fsync por append — re-mide si te importa el pico). La VM queda a ~55% de Redis.

## Cómo está hecho

- **Parser RESP2 incremental y binario-seguro** (`src/resp.ray`): consume de
  un buffer creciente y devuelve un comando por llamada o `NeedMore` con el
  frame partido por TCP; comandos inline (`PING\r\n`) incluidos.
- **Un actor dueño del keyspace** (como Redis: single-threaded donde importa):
  cada conexión es una fibra que parsea y serializa sus comandos por canal.
  Pub/sub: el actor guarda los canales de salida de los suscriptores; una
  conexión suscrita gana una fibra escritora.
- **AOF lógica**: `INCR` se persiste como el `SET` de su resultado; TTL como
  `PEXPIREAT` absoluto en epoch-ms (sobrevive reinicios con reloj de pared).
  Replay con el mismo parser del servidor (cola rota = descartada); rewrite
  compactante al arrancar con `fs.rename` atómico. `kill -9` probado.
- Expiración: perezosa en cada acceso + barrido cada 250 ms.
- `COMMAND`/`CONFIG`/`CLIENT`/`INFO` responden benignamente (el ruido de
  handshake de redis-cli/redis-benchmark).

## Estado actual

| Capacidad | Estado |
|-----------|--------|
| RESP2 incremental (multibulk + inline), binario-seguro en valores | ✅ |
| SET/GET/DEL/EXISTS/INCR*/EXPIRE*/TTL*/TYPE/KEYS(glob)/DBSIZE/FLUSHALL | ✅ |
| Pub/sub (SUBSCRIBE/UNSUBSCRIBE/PUBLISH) con limpieza al desconectar | ✅ |
| AOF lógica + replay + rewrite compactante (kill -9 probado) | ✅ |
| Compatible redis-cli, redis-benchmark y `net/redis` (tests con ambos) | ✅ |
| Binario nativo a ~86% de Redis real | ✅ |
| Tests (parser incremental, E2E con net/redis + reinicio) | ✅ 8 |
| fsync (durabilidad ante corte de luz) | ✅ (raylang M115.1: `fs.sync` por append) |
| Valores binarios en la AOF | ✅ (raylang M115.1: `fs.write_bytes`) |
| Hashes/listas/sets, RDB snapshot, replicación | 📋 fuera de v1 |

## Hallazgos de dogfood (necesidades confirmadas del lenguaje)

Anotados en `raylang/IDEAS.md` §68:

1. **Nativo: un `return;` dentro de una clausura `spawn` rompe el build**
   (E0308: el cierre generado infiere `()` y choca con `__RaySend::U`).
   Workaround: bandera de salida en vez de `return`. [RESUELTO en raylang,
   PR #140: el cuerpo del spawn se emite como IIFE y `return;` compila.]
2. **[RESUELTO — raylang M115.1]** No hay `fs.write_bytes(handle, bytes)`:
   existe, y la AOF es BINARIA (valores \x00/\xff/no-UTF-8 intactos).
3. **[RESUELTO — raylang M115.1]** La ausencia de fsync: cada append pasa por
   `fs.sync` (durable ante corte de luz).
4. **Positivo**: el pipeline bytes → parser incremental → actor → Map aguanta
   124k ops/s con 50 conexiones — el costo del round-trip por canal por
   comando es asumible incluso a escala redis-benchmark.

## Desarrollo

```sh
ray test                          # 8 tests
ray run src/main.ray --no-aof --port 7379
ray build --native src/main.ray -o raykv --release
redis-benchmark -p 7379 -t set,get -n 50000 -c 50 -q
```

Estructura: `src/main.ray` (CLI) · `resp.ray` (parser/encoders) · `store.ray`
(actor del keyspace + pub/sub + expiry) · `aof.ray` (log + rewrite) ·
`server.ray` (accept + fibra por conexión).

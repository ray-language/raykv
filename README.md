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

**Ojo con las filas sin AOF de esta tabla:** con un solo `redis-benchmark`,
el cliente va al 99–100% de CPU contra los dos servidores, así que mide el
techo del cliente, no a Redis. Medido con varios clientes y raylang 1.27.54,
raykv empata con Redis sin pipelining con `RAYLANG_THREADS=2` y se queda en
~67% con los workers por defecto (ver «Sin pipelining» más abajo). Las filas
con AOF no tienen ese problema: las limita el fsync.

### Pipelining (raylang 1.27.54)

Misma máquina, AOF off salvo donde se indica, mediana de 3 corridas,
`redis-benchmark -t set,get` con `-P` (comandos en vuelo por conexión) y `-d`
(bytes por valor; por defecto 3). «antes» es raykv sin los lotes, sin el
índice de TTL y sin el camino rápido de GET (raylang 1.27.49). Con pipelining,
Redis va al 97–99% de CPU ya con un solo cliente (con dos, ±3%): esta tabla
sí mide a los servidores.

| Carga | raykv antes SET / GET | **raykv SET / GET** (11 workers, por defecto) | raykv, `RAYLANG_THREADS=2` | Redis 8.8 SET / GET |
|---|---|---|---|---|
| `-c 50 -P 16` | 382k / 383k | **1.89M / 2.30M** | 2.04M / 2.39M | 1.60M / 2.06M |
| `-c 10 -P 1000` | 225k / 293k | **3.90M / 5.68M** | 3.56M / 4.45M | 2.92M / 4.47M |
| `-c 10 -P 1000 -d 100` | 251k / 294k | **3.72M / 5.08M** | 3.25M / 4.30M | 2.86M / 4.02M |
| `-c 50 -P 16 -d 1000` | 367k / 360k | **1.74M / 1.71M** | 1.64M / 1.89M | 1.49M / 1.90M |
| `-c 50 -P 16`, `--fsync data` | 246k / 386k | **364k / 2.23M** | 328k / 2.35M | — |

- **Pipelining por lotes.** La fibra de la conexión manda al actor, en un solo
  mensaje, todos los comandos completos de una lectura del socket, y escribe
  todas las respuestas con un solo `write`. Antes era un viaje al actor y un
  `write` por comando. `SUBSCRIBE`/`UNSUBSCRIBE`/`QUIT` cortan el lote para
  mantener el orden de las respuestas.
- **GET más rápido que SET**, como en Redis (con `-P 1000`, 1,46× frente a
  1,53×). Juntar las respuestas con `out = out + frame` copiaba todo lo
  acumulado en cada `+` (cuadrático; las respuestas de GET pesan más que el
  `+OK` de SET): con valores de 100 bytes, GET caía a ~400k. Ahora:
  - `bytes_concat` (raylang 1.27.54) une los trozos de un lote en una sola
    reserva. Hasta 1.27.53 no había forma lineal de juntar `bytes` y raykv
    los unía por pares, nivel a nivel (n·log n; hallazgo #162). Medido en la
    misma tanda, GET gana un 4–9% con valores pequeños y con 1 KB queda
    dentro del ruido; SET no cambia.
  - GET no construye su respuesta: el actor deja la cabecera (de una tabla
    `$0`…`$4095` hecha al arrancar), el valor y `\r\n` como trozos sueltos.
  - Un lote de más de 2 KB vuelve a la conexión sin unir y lo une esa fibra,
    fuera del actor, que es el único hilo que hace cola.
  - SET responde con un `+OK\r\n` constante.
  Con valores de 1 KB y 11 workers GET se queda por debajo de Redis (1,71M
  frente a 1,90M); con 2 workers empata.
- **Índice de TTL.** El barrido de cada 250 ms recorría (y ordenaba) el
  keyspace entero buscando claves caducadas: con un millón de claves bajaba
  GET de 156k a 93k. Ahora recorre solo las claves con TTL.

### Sin pipelining (raylang 1.27.54)

Con un solo `redis-benchmark` el cliente se satura (99–100% de CPU contra
cualquier servidor) y todo parece rondar los 160–180k. Con 2 o 4 procesos
`redis-benchmark` en paralelo (50 conexiones en total), mediana de 3 corridas:

| Servidor | PING, 2 clientes | SET, 2 clientes | SET, 4 clientes | SET `-r 1000000`, 2 clientes | CPU (2 clientes) |
|---|---|---|---|---|---|
| Redis 8.8 | 268k | 263k | 250k | 242k | 98% |
| servidor raylang mínimo (solo responde `+PONG`), 2 workers | 279k | — | — | — | 145% |
| ídem, 1 worker | 234k | — | — | — | 99% |
| ídem, 11 workers (por defecto) | 205k | — | — | — | 344% |
| **raykv, 2 workers** (`RAYLANG_THREADS=2`) | 267k | **266k** | 227k | **246k** | 175% |
| raykv, 4 workers | 215k | 212k | 196k | — | 280% |
| raykv, 1 worker | 202k | 198k | 182k | — | 99% |
| raykv, 11 workers (por defecto) | 178k | 176k | 170k | 170k | 535% |

- **raylang 1.27.54 quitó el techo del runtime.** Hasta 1.27.53 un servidor
  raylang que no hacía nada no pasaba de ~200k: cada vez que una fibra
  aparcaba esperando su socket, avisaba por pipe a un hilo reactor, que
  gastaba 3,6 µs de `kevent` por petición (hallazgo #163). Ahora cada worker
  sondea su propio `kqueue`. Con 1 worker, el servidor mínimo gasta 4,2 µs
  de CPU por petición (antes 8,3; Redis 3,6), casi todo en `recvfrom` y
  `sendto`.
- **El número de workers decide.** Con 2 workers raykv empata con Redis con
  2 clientes y llega al 91% con 4. Con los 11 por defecto (uno por núcleo),
  cada worker atiende muy pocas conexiones, pasa el 80–90% del tiempo en
  `kevent` sin trabajo, y raykv rinde un tercio menos con 3× la CPU. Un solo
  worker se satura. Lo que le cuesta a raykv su propio trabajo (RESP, el
  salto al actor, el Map) es ~5% frente al servidor mínimo con 2 workers.
- **Qué usar.** `RAYLANG_THREADS=2` para muchas peticiones pequeñas sin
  pipelining, que es lo que hace un cliente normal. Con lotes grandes
  (`-P 1000`) rinden más los workers por defecto, aunque 2 siguen por encima
  de Redis en SET y empatan en GET. Con `--fsync data`, 2 workers pierden un
  10% de SET con `-P 16` (328k frente a 364k) y ganan en GET.

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
| Binario nativo a la par de Redis sin pipelining con `RAYLANG_THREADS=2` (~67% con los 11 workers por defecto); con pipelining por encima de Redis, salvo GET con valores de 1 KB y 11 workers | ✅ |
| Tests (parser incremental, actor, lotes, barrido de TTL, AOF, E2E con net/redis + pipelining + reinicio) | ✅ 16 |
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

Requiere raylang ≥ 1.27.54 (`bytes_concat`); la dependencia `net` viene del
índice de paquetes (`net = "^0.3.7"` en `ray.toml`, versión exacta fijada en `ray.lock`).

```sh
ray test                          # 16 tests
ray run src/main.ray --no-aof --port 7379
ray run src/main.ray --fsync full --port 7379   # AOF que sobrevive a un corte de luz
ray build --native src/main.ray -o raykv --release
redis-benchmark -p 7379 -t set,get -n 50000 -c 50 -q
```

Estructura: `src/main.ray` (CLI) · `resp.ray` (parser/encoders) · `store.ray`
(actor del keyspace + pub/sub + expiry) · `aof.ray` (log + rewrite) ·
`server.ray` (accept + fibra por conexión).

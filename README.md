# polea

Un broker de mensajería **pub/sub** minimalista y distribuible, escrito en
**C++17** sin dependencias externas. polea reenvía mensajes entre "pozos"
publicadores y "poleas" suscriptoras mediante un protocolo binario propio,
delimitado por longitud y protegido con **CRC-32**, sobre TCP.

El nombre viene de la polea (la rueda que transmite el movimiento de un eje a
otro): lo que un proceso publica en un tema, la polea lo transmite a todos los
procesos que lo están escuchando.

## Características

- Protocolo binario v1 con cabecera fija (13 B), longitudes explícitas y
  CRC-32: cada trama es auto-delimitable y a prueba de corrupción.
- Reenvío ordenado y determinista: un único hilo con `poll(2)` garantiza el
  orden global de los mensajes.
- Filtrado por tema (`topic`): cada suscriptor recibe solo lo que le interesa.
- **Backpressure con colas acotadas**: una cola por cliente; si se llena, se
  descarta el mensaje más antiguo, contado en la estadística de descartes.
- Reconocimientos opcionales (`ack`): el publicador puede pedir confirmación
  con el id asignado.
- estados en vivo: `polea status` muestra peers, temas, mensajes encaminados,
  descartes y el próximo id.
- Caída **limpia** de clientes que violan el protocolo (trama ilegible, CRC
  erróneo, Hello duplicado, tipo desconocido) sin afectar al resto.
- Sin dependencias: solo C++17 estándar y `poll(2)`. Compila con g++.

## Compilar y probar

```sh
make            # compila el binario `polea`
make test       # 22 pruebas de integración reales sobre sockets localhost
make test-asan  # mismo conjunto bajo -fsanitize=address,undefined
```

El conjunto de pruebas arranca el servidor dentro de un hilo y conecta
clientes TCP reales en `127.0.0.1`, verificando orden de entrega, exclusión de
no suscriptores, autoentrega, bajas, descartes por backpressure, corrupción de
CRC y tráfico con muchos clientes.

## Uso

```sh
# 1. Arranca un broker en el puerto 4590
polea serve                    # -p puerto, -q silencioso, -t cola, -i idle

# 2. Un suscriptor escucha uno o varios temas
polea listen 127.0.0.1:4590 radar alertas -n estación

# 3. Un publicador suelta un mensaje (opcionalmente pidiendo confirmación)
polea say 127.0.0.1:4590 radar "avistamiento a las 14:32" -a

# 4. Estado del broker
polea status 127.0.0.1:4590
```

Salida esperada del suscriptor:

```
[polea] conectado; id=1
[polea] suscrito a «radar»
[radar] avistamiento a las 14:32
```

### Protocolo (v1)

Cada trama (todo *little-endian*):

```
+--------+---------+-------+--------+------------+---------------+-------+
| magic  | version | type  | flags  | topic_len  |  payload_len  | topic |
| 4 B    | 1 B     | 1 B   | 1 B    | 2 B        |  4 B          | N B   |
+--------+---------+-------+--------+------------+---------------+-------+
| payload (P B)                       | crc32 (4 B sobre version..payload) |
+-------------------------------------+------------------------------------+
```

- `magic` = `A7 11 B5 7E`: permite detectar la sincronización de trama.
- El **CRC** cubre desde `version` hasta el final del payload, de modo que un
  byte perdido o alterado invalida la trama y desconecta al cliente culpable.
- Tipos: `Hello`, `HelloAck`, `Sub`, `Unsub`, `Pub`, `Deliver`, `PubAck`,
  `List`/`ListReply`, `Error`, `Ping`/`Pong`, `StatsReq`/`StatsReply`.
- `Deliver` antepone 8 B con el id de mensaje global (LE) al payload original.
- Límites: tema ≤ 64 B, payload ≤ 1 MiB.

## Cómo se implementó

- `src/protocol.{h,cpp}`: codificación/decodificación estricta con CRC-32.
- `src/crc32.{h,cpp}`: CRC-32 polinómico tabla por byte.
- `src/polea_server.{h,cpp}`: bucle `poll(2)` de un hilo, colas acotadas,
  expiración por inactividad y contadores de encaminamiento.
- `src/polea_client.{h,cpp}`: cliente bloqueante con rebanado de tramas.
- `src/main.cpp`: interfaz de línea de comandos (`serve`/`listen`/`say`/`status`).
- `tests/test.cpp`: 22 pruebas de integración contra el broker real.

## Licencia

MIT — ver `LICENSE`.
# Ephemeral Reset (Prometheus + Grafana + Kafka)

Use this reset when you want a fully clean visualization baseline.

## 1) Stop and remove containers + volumes

```bash
docker compose down -v
```

## 2) Remove local Kafka data folders

```bash
rm -r ./kafka1/data ./kafka2/data ./kafka3/data
mkdir -p ./kafka1/data ./kafka2/data ./kafka3/data
```

PowerShell equivalent:

```powershell
Remove-Item -Recurse -Force .\kafka1\data,.\kafka2\data,.\kafka3\data
New-Item -ItemType Directory .\kafka1\data,.\kafka2\data,.\kafka3\data | Out-Null
```

## 3) Start fresh stack

```bash
docker compose up -d
```

## 4) Verify clean state

- Prometheus targets: `http://localhost:9090/targets` -> all expected targets are `UP`
- Grafana dashboard: `http://localhost:3000` -> `Kafka Idempotency Overview` loads
- No old run IDs in latest time range before new test starts

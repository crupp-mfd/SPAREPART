# AppTeilenummer

Eigenstaendige MFDApps-App im Monorepo.

## Lokal starten

```bash
./apps/AppTeilenummer/dev-server.sh --port 8000
```

## Deploy (Azure Container Apps)

```bash
./apps/AppTeilenummer/deploy.sh
```

Optionale Env-Variablen:

- `APP_NAME`
- `RESOURCE_GROUP`
- `ACR_NAME`
- `IMAGE_REPO`
- `TAG`

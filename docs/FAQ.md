# Validation Rules

The application validates all configuration on startup. If validation fails, the application will not start and will print detailed error messages.

## Common Validation Errors

- **Missing required fields**: All fields marked as "Required: Yes" must be provided
- **Missing database credentials**: `postgres_url` or `cockroach_url` must be set based on `datastore`
- **Invalid port numbers**: Ports must be between 1 and 65535
- **Invalid log level**: Must be one of: `debug`, `info`, `warn`, `error`
- **Invalid datastore**: Must be `postgres` or `cockroach`
- **Invalid buffer type**: Must be `memory` or `disk`
- **Missing WAL path**: Required and non-empty when `buffer_type` is `disk`
- **Invalid jitter percentage**: Must be between 5 and 100
- **Invalid intervals**: Most intervals have minimum values (e.g., 1000ms for buffer flush)
- **API key too short**: Must be at least 32 characters for security
- **Out of range values**: Check validation column for each field's allowed range
- **Connection pool limits**: Max open/idle connections must be between 1 and 100

import secrets

# Generate a random secure key
secret_key = secrets.token_urlsafe(32)  # 32 bytes key, URL-safe base64
print(secret_key)
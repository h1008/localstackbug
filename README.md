# Localstack Bug

Running against localstack:

```sh
docker-compose up

AWS_ACCESS_KEY_ID="test" AWS_SECRET_ACCESS_KEY="test" AWS_ENDPOINT_URL=http://localhost:4566 go test -v
```

Running against real AWS account:

```sh
aws-vault exec <profile> -- go test -v
```
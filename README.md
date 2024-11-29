
# How to use it

1. set git.silverwhisper.pl as private repository using environment variable:

```
go env -w GOPRIVATE=git.silverwhisper.pl
```

2. setup credentials in `~/.netrc` (Linux) or `$HOME/_netrc` (Windows) file:

```
machine git.silverwhisper.pl login <user_name> password <gitlab-rw-token>
```

WARNING: password/token is unencrypted

3. install module:

```
go get git.silverwhisper.pl/golang-common-modules/rabbitmq
```

4. use module in go ...


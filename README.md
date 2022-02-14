# mtor
A script can cold migrate data from Mysql to redis.

## How to use this tool
Just run `ctl/mtor-ctl.go`, and change the 16th and 17th lines.
The 16th line mean : the `.sql` file's path, and the 17th line mean : the `.dump` files' path.

## Attention
As of now, this is a unfinished project, it just support single database once time. More importantly, I DON'T test the result if right!

## Design document
[【腾讯文档】SQL to Redis Implementation](https://docs.qq.com/doc/DVWNQQnVHUWFHYW5s)

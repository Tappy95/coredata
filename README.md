## MQ
```
nsq:
    address:
        http://134.73.133.2:25751
example:
    curl -d "<message>" http://134.73.133.2:25751/pub?topic=name
```

## Amazon任务
#### amazon类目同步
```
任务:
    mq:
        nsq:
            topic:
                haiying.amazon.category
    数据:
        {
            "task": "amazon_category_sync"
            "data": {
                "site": "us",
            }
        }
```

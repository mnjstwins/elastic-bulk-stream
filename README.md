# elastic-bulk-stream
transform stream to transfer data to elasticsearch

```js

import { Client } from 'elasticsearch'
import elasticBulkStream from 'elastic-bulk-stream'

const client = new Client({ host: 'http://localhost:9200' })

getSomeInputStream().pipe(elasticBulkStream(client))
```



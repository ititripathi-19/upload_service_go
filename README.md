## Read a CSV file of Mobile Numbers, Generate md5 hash of Number and produce the combine data to kafka

### Execute/Run
  - go run main.go

### Program Flow
1. It will read the file present in the data folder and send the records to 'records' channel
2. It will start mentioned gorouties to read from 'records' channel and generate a md5 hash for each number and send it to 'kafkaRecords' channel 
3. It will start the mentioned gorouties to read from 'kafkaRecords' and product into kafka and send status to 'status' channel based on produce result
4. It will calculate the success and failed counts coming from 'status' channel and aggreate them. 

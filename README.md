# Demonstration Code for Textract and Comprehend
This example will process a number of documents from a given directory and create an asyncrhonous
job in Textract to process those documents. The code will then process the resultant output (in
JSON) and run it through the Amazon Comprhend Service to detect Entitles. It will then store the
results an a CSV for further analyis in the given S3 bucket.

# Usage

```bash
textractandcomprehend.py ---directory <local directory of documents> --bucket <amazon S3 bucket name> 
```

from string import Template


lambda_stream_policy = Template("""{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "dynamodb:PutItem",
            "dynamodb:DeleteItem",
            "dynamodb:UpdateItem"
        ],
        "Resource": "$newtable"
    }, {
        "Effect": "Allow",
        "Action": [
            "dynamodb:UpdateItem"
        ],
        "Resource": "$oldtable"
    }, {
        "Effect": "Allow",
        "Action": [
            "dynamodb:GetShardIterator",
            "dynamodb:DescribeStream",
            "dynamodb:ListStreams",
            "dynamodb:GetRecords"
        ],
        "Resource": "$oldtablestream"
    }, {
        "Effect": "Allow",
        "Action": "logs:*",
        "Resource": "*"
    }
]}""")

lambda_stream_assume_role = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""

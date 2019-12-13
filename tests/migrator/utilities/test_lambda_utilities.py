from migrator.utilities.LambdaUtilities import update_boto_client_endpoints


def test_none_returns_none():
    assert update_boto_client_endpoints(None, None) is None
    assert update_boto_client_endpoints("", None) == ""


def test_code_without_clients_returned_as_is():
    existing_code = """
    def handler():
        return 'some data'
    """
    assert update_boto_client_endpoints(existing_code, None) == existing_code


def test_code_with_one_client_returned_with_specified_endpoint():
    existing_code = """
        import boto3
        def handler():
            client = boto3.client('dynamodb2')
            return 'some data'
        """
    location_url = "http://localhost:5000"
    expected_code = f"""
        import boto3
        def handler():
            client = boto3.client('dynamodb2', endpoint_url='{location_url}')
            return 'some data'
        """
    assert update_boto_client_endpoints(existing_code, location_url) == expected_code


def test_code_with_multiple_clients_returned_with_specified_endpoint():
    existing_code = """
            import boto3
            def handler():
                client = boto3.client('dynamodb2')
                client = boto3.client("sqs")
                return 'some data'
            """
    location_url = "http://localhost:5000"
    expected_code = f"""
            import boto3
            def handler():
                client = boto3.client('dynamodb2', endpoint_url='{location_url}')
                client = boto3.client("sqs", endpoint_url='{location_url}')
                return 'some data'
            """
    assert update_boto_client_endpoints(existing_code, location_url) == expected_code


def test_code_with_multiple_clients_with_existing_endpoint_returned_with_specified_endpoint():
    existing_code = """
                import boto3
                def handler():
                    client = boto3.client('dynamodb2', endpoint_url="http://dynamodb.aws.amazon.com")
                    client = boto3.client("sqs")
                    return 'some data'
                """
    location_url = "http://localhost:5000"
    expected_code = f"""
                import boto3
                def handler():
                    client = boto3.client('dynamodb2', endpoint_url='{location_url}')
                    client = boto3.client("sqs", endpoint_url='{location_url}')
                    return 'some data'
                """
    assert update_boto_client_endpoints(existing_code, location_url) == expected_code

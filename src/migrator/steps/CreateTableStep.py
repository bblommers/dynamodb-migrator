from migrator.utilities.AwsUtilities import AwsUtilities


class CreateTableStep:

    def __init__(self, identifier, version, properties):
        self._identifier = identifier
        self._version = version
        self._properties = properties
        self.aws_utils = AwsUtilities(self._identifier, version=self._version)

    def execute(self):
        return self.aws_utils.create_table_if_not_exists(self._properties)

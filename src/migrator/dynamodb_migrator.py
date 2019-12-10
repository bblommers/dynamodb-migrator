import os
from migrator.exceptions.MigratorScriptException import MigratorScriptException
from migrator.steps.CreateTableStep import CreateTableStep
from migrator.steps.AddIndexStep import AddIndexStep
from migrator.utilities.Utilities import logger


class Migrator:

    def __init__(self, identifier = None):
        self._functions = []
        self._steps = []
        self._version = None
        self._current_identifier = identifier if identifier else os.path.basename(__file__)
        self._table_created = False

    def version(self, version_number):
        self._version = version_number

        def inner_function(func):
            pass
        return inner_function

    def create(self, **kwargs):
        if self._table_created:
            logger.error("Unable to execute script")
            logger.error("Ensure that you have only one create-annotation per script")
            logger.error("Each table should have it's own script")
            raise MigratorScriptException("Unable to create multiple tables per script")

        def inner_function(function):
            created_table = CreateTableStep(identifier=self._current_identifier,
                                            version=self._version,
                                            properties=kwargs).execute()
            self._table_created = True
            return function(created_table)
        return inner_function

    def add_indexes(self, **kwargs):
        def inner_function(function):
            created_table = AddIndexStep(identifier=self._current_identifier,
                                         version=self._version,
                                         properties=kwargs).execute()
            return function(created_table)
        return inner_function

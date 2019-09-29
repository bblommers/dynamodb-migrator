import logging
import os
from functools import wraps
from migrator.exceptions.MigratorScriptException import MigratorScriptException
from migrator.steps.BaseStep import BaseStep
from migrator.steps.CreateTableStep import CreateTableStep


class Migrator():
    _metadata_table_name = 'dynamodb_migrator_metadata'
    _ch = logging.StreamHandler()
    _formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
    _logger = logging.getLogger('dynamodb_migrator_library')

    def __init__(self, identifier = None):
        self._ch.setFormatter(self._formatter)
        self._logger.addHandler(self._ch)
        self._logger.setLevel(logging.DEBUG)
        self._steps = []
        self._steps.append(BaseStep())
        self._current_identifier = identifier if identifier else os.path.basename(__file__)
        self._table_created = False

    def version(self, version_number):
        def inner_function(function):
            @wraps(function)
            def wrapper(*args, **kwargs):
                self.function(*args, **kwargs)
            return wrapper
        return inner_function

    def create(self, **kwargs):
        if self._table_created:
            self._logger.error("Unable to execute script")
            self._logger.error("Ensure that you have only one create-annotation per script")
            self._logger.error("Each table should have it's own script")
            raise MigratorScriptException("Unable to create multiple tables per script")

        def inner_function(function):
            self._steps.append(CreateTableStep(identifier=self._current_identifier,
                                               properties=kwargs,
                                               func=function))
            self._table_created = True
        return inner_function

    def migrate(self):
        if not self._steps:
            self._logger.warning("No migration-steps have been found")
        for step in self._steps:
            step.execute()

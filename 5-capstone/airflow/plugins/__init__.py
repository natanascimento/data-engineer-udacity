from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class FinancialPlugin(AirflowPlugin):
    name = "financial_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.FinancialOperator
    ]
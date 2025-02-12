from __future__ import division, absolute_import, print_function
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from final_project_operators.stage_redshift import StageToRedshiftOperator

import final_project_operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        final_project_operators.StageToRedshiftOperator,
        final_project_operators.LoadFactOperator,
        final_project_operators.LoadDimensionOperator,
        final_project_operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]

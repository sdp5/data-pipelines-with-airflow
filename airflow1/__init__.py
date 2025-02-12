from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

# Import directly from their respective modules
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

# Import helper module
import final_project_operators
import helpers


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        final_project_operators.StageToRedshiftOperator,  # Use the direct class reference
        final_project_operators.LoadFactOperator,
        final_project_operators.LoadDimensionOperator,
        final_project_operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]

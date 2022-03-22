from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import helpers

#Define the plugin class
class MyPlugins(AirflowPlugin):
    #Name your AirflowPlugin
    name = "my_plugin"
    
    #List all plugins you want to use in dag operation
    helpers = [helpers.SqlQueries]
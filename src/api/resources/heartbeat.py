"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from flask_restful import Resource


class Heartbeat(Resource):
    """
    Defines the Web API for checking whether the Imaging Node does respond.
    """

    def get(self):
        """
        Provides a standard OK response only.
        :return: a simple OK message with HTTP status of 200.
        """
        return 'OK', 200
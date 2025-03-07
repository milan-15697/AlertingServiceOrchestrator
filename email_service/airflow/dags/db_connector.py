"""Kafka, Postgresql, Ignite connetion hub"""
import os
import logging
import psycopg2

class DB_Connector():
    """Class responsible for gathering connections"""
    logger = logging.getLogger('master')

    def connect_to_psql(self,target_db=os.environ['HOTPATH_DB_NAME']):
        """Returns a postgres connection object"""
        try:
            return psycopg2.connect("dbname="+target_db+\
                " user="+os.environ['HOTPATH_DB_USER']+" password="+\
                os.environ['HOTPATH_DB_PASS']+" host="+os.environ['HOTPATH_DB_HOST'])
        except (Exception,psycopg2.OperationalError) as ex:
            self.logger.exception('Error while connection with Postgresql %s',ex)

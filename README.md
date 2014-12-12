mondriandategenerator
=====================

A DSP to create a dynamic "in query" date dimension on the fly in your schema. 

MONDRIAN 4 ONLY!

Add to connection string like so:

jdbc:mondrian:Jdbc=jdbc:mysql://localhost/foodmart;Catalog=mondrian:///datasources/foodmart4.xml;JdbcDrivers=com.mysql.jdbc.Driver;DynamicSchemaProcessor=bi.meteorite.App;StartDate=19970101;EndDate=19981231;Cubes=Sales=the_date,Warehouse=time_id

StartDate, EndDate and Cubes String Mandatory to define the size of the dimension.
If you have an incremental id field you'd rather link to (which is much more performant) use InitID parameter to pass a starting integer.

This is supposed to make Proof Of Concepts and Demo's much quicker to get off the ground, it is not supposed to provide the same level of performance that a table in a database can provide and joining 2 date fields is pretty rubbish.

Tested on MySQL, log bugs at http://jira.meteorite.bi

# movieswebapp
This is a web app which displays suggestions for films based on filters that the user can use to input their preferences.
It is based on an ETL data pipeline which takes csv data from streaming services as a csv file, formats the data using PySpark transformations and then uploads 
it to a local MySQL server.
The flask app takes the data from this server and displays it in a table, with various dropdown menus which allow the user to choose the audience, runtime,
streaming service and whether the film is american or not.

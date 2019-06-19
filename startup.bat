set DIRNAME=.\
cd %DIRNAME%

@set CMD=java -jar

 

start /i          %CMD% --serverPort=8775
start /i          %CMD% --serverPort=8776
start /i          %CMD% --serverPort=8777

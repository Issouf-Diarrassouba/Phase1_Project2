To run Airflow:

From powershell: 
run: docker compose up -d
Wait 20 seconds, type localhost:8080 into search bar of browser
Username is admin, password is in powershell with command: docker compose logs airflow | Select-String -Pattern "password"
Go to DAG tab and run dag
To close pipeline, run: docker compose down

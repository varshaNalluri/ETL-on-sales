# ETL-on-sales

This project performs an ETL (Extract, Transform, Load) process on sales data using Python.

## Project Structure

- `etl_pipeline.py`: Contains the ETL pipeline logic.
- `schedule.py`: Schedules the ETL pipeline.
- `dashboard.py`: Visualizes the sales data using graphs.
- `transformed_data.csv`: Output data after transformation.
- `Warehouse_and_Retail_Sales.csv`: Input data.
- `etl_pipeline.log`: Logs generated during ETL execution.
- `unittests/`: Contains unit tests for the pipeline.

## Requirements
Install the required dependencies using:
```bash
pip install -r requirements.txt
```

## Running the ETL Pipeline
To execute the ETL pipeline:
```bash
python etl_pipeline.py
```

## Schedule the Pipeline
You can run the scheduler with:
```bash
python schedule.py
```

## Visualize Data
Run the dashboard using:
```bash
python dashboard.py
```

## Testing
Run unit tests using:
```bash
pytest unittests/
```

## License
This project is licensed under the MIT License.


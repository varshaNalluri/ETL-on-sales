import unittest
import pandas as pd
from etl_pipeline import extract_data, transform_data, load_data  # Import your functions

class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        """Set up a sample DataFrame for testing."""
        self.sample_data = pd.DataFrame({
            'SUPPLIER': ["REPUBLIC NATIONAL DISTRIBUTING CO", "PWSWN INC", "RELIABLE CHURCHILL LLLP"],
            'ITEM TYPE': ["WINE", "BEER", "LIQUOR"],
            'RETAIL SALES': [100, 200, 300],
            'RETAIL TRANSFERS': [50, 60, 70],
            'WAREHOUSE SALES': [30, 40, 50]
        })

    def test_extract(self):
        """Test extraction from CSV"""
        df = extract_data("ETL/Warehouse_and_Retail_Sales.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

    def test_transform(self):
        """Test transformation logic"""
        transformed_df = transform_data(self.sample_data)
        self.assertIn('TOTAL SALES', transformed_df.columns)
        self.assertIn('PROFIT MARGIN', transformed_df.columns)
        self.assertAlmostEqual(transformed_df.loc[0, 'TOTAL SALES'], 180)  # 100+50+30
        self.assertAlmostEqual(transformed_df.loc[0, 'PROFIT MARGIN'], round(100/180, 2))

    def test_load(self):
        """Test if transformed data is saved correctly"""
        transformed_df = transform_data(self.sample_data)
        load_data(transformed_df, "test_output.csv")
        loaded_df = pd.read_csv("test_output.csv")
        self.assertFalse(loaded_df.empty)

if __name__ == "__main__":
    unittest.main()

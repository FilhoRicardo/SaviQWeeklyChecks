# Step 1: Import the class
from energy_analysis.dexcell_extractor import DexcellDataExtractor

# Step 2: Create an instance (object) of the class
# This is called "instantiating" the class
extractor = DexcellDataExtractor('amalpha_config.json', debug=True)

# What we just did:
# - Created an object called 'extractor' from the DexcellDataExtractor class
# - Passed 'team_config.json' as a property to configure the object
# - Set debug=True to see detailed logs (optional)

# Step 3: Call methods to perform actions

# Method 1: extract_data()
# This method fetches data from the API for all configured devices
data = extractor.extract_data()
# The 'data' variable now contains a list of dictionaries with the extracted data

# Method 2: save_to_csv()
# This method saves the extracted data to a CSV file
extractor.save_to_csv('monthly_output_data.csv')
# A file named 'output_data.csv' is now created with your data

# Method 3: get_summary()
# This method returns a summary of the extraction
summary = extractor.get_summary()
print(summary)
# This prints information about what was extracted
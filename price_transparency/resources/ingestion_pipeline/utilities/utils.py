from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, MapType, StringType
import requests

# @staticmethod
@udf(MapType(StringType(), StringType()))
def copy_file(src_path, dest_path):
    try:
    with open(src_path, 'rb') as src_file, open(dest_path, 'wb') as dest_file:
        dest_file.write(src_file.read())
    return {"status": "success"}
    except Exception as e:
    return {"status": "error", "message": str(e)}

# @staticmethod
@udf(MapType(StringType(), StringType()))
def download_file(url, dest_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Extract the original file name from the URL
        original_name = url.split('/')[-1]
        
        # Determine the new file name
        if '.json.gz' in original_name:
            new_name = original_name.split('.json.gz')[0]
        elif '.json' in original_name:
            new_name = original_name.split('.json')[0]
        else:
            new_name = original_name
        
        # Update the destination path with the new file name
        dest_path = dest_path.replace(original_name, new_name)
        
        with open(dest_path, 'wb') as dest_file:
            dest_file.write(response.content)
        
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

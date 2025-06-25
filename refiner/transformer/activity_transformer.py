import csv
from typing import Dict, Any, List
from refiner.models.refined import Base, UserActivity
from refiner.transformer.base_transformer import DataTransformer

class ActivityTransformer(DataTransformer):
    """
    Transformer for ViewingActivity.csv data.
    """
    def transform(self, data: Dict[str, Any]) -> List[Base]:
        csv_path = data['csv_path']
        address = data['address']
        activities = []
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                activity = UserActivity(
                    address=address,
                    profile_name=row['Profile Name'],
                    start_time=row['Start Time'],
                    duration=row.get('Duration'),
                    attributes=row.get('Attributes'),
                    title=row.get('Title'),
                    supplemental_video_type=row.get('Supplemental Video Type'),
                    device_type=row.get('Device Type'),
                    bookmark=row.get('Bookmark'),
                    latest_bookmark=row.get('Latest Bookmark'),
                    country=row.get('Country')
                )
                activities.append(activity)
        return activities 
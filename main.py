from pymongo import MongoClient
from dateutil import parser, rrule
from bson import ObjectId
import json

def get_data_from_database():
    client = MongoClient('mongodb://localhost:27017/salary_db.my_salary_collection')
    db = client['salary_db']
    collection = db['my_salary_collection']


    data = collection.find_one({'_id': ObjectId('653d3ac86df43e9a7810cf48')})
    if data:
        dt_from = data['date']['$date']
        return dt_from
    else:
        return None

def aggregate_salaries(dt_from, dt_upto, group_type, collection):
    try:

        dt_from = parser.parse(dt_from)
        dt_upto = parser.parse(dt_upto)


        if group_type == 'month':
            frequency = rrule.MONTHLY


        dataset = []
        labels = []

        for dt in rrule.rrule(frequency, dtstart=dt_from, until=dt_upto):
            # Агрегировать данные в соответствии с диапазоном дат
            next_dt = rrule.rrule(frequency, dtstart=dt, count=1).after(dt)
            result = collection.aggregate([
                {'$match': {
                    'date': {'$gte': dt, '$lt': next_dt},
                }},
                {'$group': {'_id': None, 'total': {'$sum': '$salary'}},
                }
            ])
            total = next(result, {'total': 0})['total']
            dataset.append(total)
            labels.append(dt.isoformat())

            collection.insert_one({'date': dt, 'total_salary': total})

        result_data = {'dataset': dataset, 'labels': labels}

        return json.dumps(result_data)
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")


if __name__ == '__main':
    dt_from = get_data_from_database()  # Получение даты из базы данных
    dt_upto = "2023-01-31"
    group_type = "day"

    if dt_from is not None:
        result = aggregate_salaries(dt_from, dt_upto, group_type)
        print(result)
    else:
        print("Данные не найдены в базе данных.")

from faker import Faker
import random


fake_data = Faker()


def create_data():
    data = {
        'Name': fake_data.name(),
        "Phone_numbers": fake_data.city(),
        "date": str(fake_data.date()),
        'Address': fake_data.address(),
        'Email': fake_data.email(),
        'Country': fake_data.country(),
        'Customer_id': str(random.randint(1, 5))
    }
    return data


if __name__ == '__main__':
    create_data()

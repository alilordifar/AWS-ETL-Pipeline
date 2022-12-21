from faker import Faker


fake_data = Faker()


def create_data():
    data = {
        'Name': fake_data.name(),
        'Address': fake_data.address(),
        'Email': fake_data.email(),
        'Country': fake_data.country()
    }
    return data


if __name__ == '__main__':
    create_data()

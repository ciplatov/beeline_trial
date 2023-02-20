# Тестовое задание beeline DE

## Задание
Входные данные:
Customer.csv – информация о клиентах
Имя поля: формат

id: Int,
name: String,
email: String,
joinDate: Date,
status: String

Product.csv – информация о товарах

id: Int
name: String 
price: Double 
numberOfProducts: Int

Order.csv – информация о заказах

customerID: Int
orderID: Int
productID: Int
numberOfProduct: Int – кол-во товара в заказе
orderDate: Date
status: String

Необходимо написать Spark приложение на scala (предпочтительно) или python, которое будет выполнять следующие действия – 
1.	Чтение входных данных
2.	Определение самого популярного продукта для каждого клиента (итоговое множество содержит поля: customer.name, product.name)
3.	Запись результата в файл csv


## Предварительный анализ данных

1. В поле status таблицы Order присутствуют значения: delivered, canceled, error. Т.к. нужно найти самый популярный продукт, используем только значение delivered
2. Если получится, что несколько товаров покупатель приобретал одинаковое кол-во раз, то выводим товар идущий раньше по алфавиту. (Стоит уточнить в ТЗ)

## Запуск
Для получения результата в виде csv файла необходимо склонировать репозиторий, после чего выполнить следующие команды:

```
docker build -t beeline_trial .
docker run -v $(pwd)/out:/out beeline_trial /opt/spark/bin/spark-submit /src/main.py
```

Файл csv с результатом расчетов будет находиться в директории out


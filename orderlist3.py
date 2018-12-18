import os

import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import decimal


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="6666"
)
cur = conn.cursor()

# использование созданной БД
cur.execute("""USE OrderLog;""")

# считываем тикеры из файла
tickers_text = open('ListingSecurityList.csv', encoding = "cp1251").read()
tickers = {}
for elem in tickers_text.split('\n')[:-1]:
    t = elem.split(',')
    tickers[t[7].replace("\"", "")] = t[5].replace("\"", "")

ticker = input("Введите тикер: ")
# пример формата времени (для шага и границ периода): 101001000000 (10 часов 10 минут 1 секунда)
step = input("Введите шаг: ")
time_lower = int(input("Введите начало периода: "))
time_upper = int(input("Введите конец периода: "))
volume = int(input("Введите торговый объем сделки : "))
deal_type = input("Введите тип сделки : ")

# Переведем переменную step в формат, который позволит суммировать время
step = datetime.strptime(step, '%H%M%S%f')
# Создаем фиктивную переменную, которая позволит суммировать время
time_zero = datetime.strptime('000000000', '%H%M%S%f')

# Создаем списки для переменных, графики которых нужно изобразить
mid_price = []
bid_ask_spread = []
depth_ask = []
depth_bid = []
av_prices = []
# Создаем список для моментов времени
time_list = []
# Приравниваем первый момент времени к началу периода
time = time_lower


# цикл будет выполняться до тех пор, пока момент времени не "превысит" конец периода
while time<=time_upper:
    # с учетом того, что в исходном txt файле с заявками последние цифры времени - миллисекунды,
    # а в питоне можно работать только в формате микросекунд (дополнительно еще три цифры), удалим последние три цифры и добавим данный
    # момент времени в соответствующий лист
    time_list.append(int(str(time)[:-3]))
    # в зависимости от типа инструмента выгружаем информацию о заявках до указанного момента времени
    if tickers[ticker] == 'Акция обыкновенная':
        t = f"""SELECT orderno, action, buysell, price, volume, time FROM CommonStock
where TIME<= '%s' and seccode = '%s';""" % (int(str(time)[:-3]), ticker)
        cur.execute(t)
        rows = cur.fetchall()
    elif tickers[ticker] == 'Акция привилегированная':
       t = f"""SELECT orderno, action, buysell, price, volume, time FROM PreferredStock
where TIME<= '%s' and seccode = '%s';""" % (int(str(time)[:-3]), ticker)
       cur.execute(t)
       rows = cur.fetchall()
    elif 'облигац' in tickers[ticker].lower():
       t = f"""SELECT orderno, action, buysell, price, volume, time FROM Bonds
where TIME<= '%s' and seccode = '%s';""" % (int(str(time)[:-3]), ticker)
       cur.execute(t)
       rows = cur.fetchall()

    # создаем спискок для заявок
    glass = [] #(buysell, price, volume)
    # создаем временный спискок для цен, по которым был реализован объем (указанный пользователем) до данного момента времени
    prices = []

    for elem in rows:
        # если action==1 (размещение заявки)
        if elem[1] == 1:
            # если объем не равен нулю
            if elem[4] != 0:
                # если список непустой
                if len(glass)>0:
                    flag = False
                    for j in glass:
                        # если в списке уже есть заявка такого же типа (buy/sell) с такой же ценой
                        if j[1] == elem[3] and j[0] == elem[2]:
                            # добавляем к исходному объему объем данной заявки
                            j[2] += elem[4]
                            flag = True
                            # выходим из цикла
                            break
                    # в противном случае добавляем заявку в стакан
                    if not flag:
                        glass.append([elem[2], elem[3], elem[4]])
                # если список пустой, добавляем заявку (первая заявка)
                else:
                    glass.append([elem[2], elem[3], elem[4]])
            else:
                pass
        # если action=0 (снятие заявки)
        elif elem[1] == 0:
            # ищем в стакане нужную заявку
            for j in glass:
                # если тип заявки (buy/sell) и цена совпадают, значит мы нашли нужную заявку
                if j[1] == elem[3] and j[0] == elem[2]:
                    # вычитаем соответствущий объем
                    j[2] += -elem[4]
                    # если оставшийся объем равен нулю, удаляем заявку
                    if j[2] <= 0:
                        del(j)
                    # так как мы нашли нужную заявку, выходим из цикла
                    break
        # если action=2 (сделка)
        elif elem[1] == 2:
            # ищем в стакане нужную заявку
            for j in glass:
                # если тип заявки (buy/sell) и цена совпадают, значит мы нашли нужную заявку
                if j[1] == elem[3] and j[0] == elem[2]:
                    # вычитаем соответствущий объем
                    j[2] += -elem[4]
                    # если оставшийся объем меньше или равен нулю, удаляем заявку
                    if j[2] <= 0:
                        del(j)
                    # если тип сделки и объем совпадают с теми, которые указывал пользователь, добавляем цену сделки в соответствующий список
                    if deal_type == elem[2] and volume == elem[4]:
                        prices.append(elem[3])
                    # выходим из цикла
                    break

    # если мы нашли сделки, у которых тип и объем совпадали с теми, которые указывал пользователь, добавляем усредненное значение цен в
    # соответствующий список
    if len(prices)>0:
        av_prices.append(np.mean(np.array(prices)))
    else:
        # в противном случае добавляем ноль
        av_prices.append(0)

    # "очищаем" стакан от заявок с нулевым объемом
    glass_new = []
    for j in range(0,len(glass)):
        if glass[j][2]!=0:
            glass_new.append(glass[j])

    # Из списка glass_new создадим датафрейм
    df = pd.DataFrame.from_records(glass_new, columns=['buy/sell', 'price', 'volume'])
    # создадим отдельные колонки для объема ask и bid (ноль в i-ой строке означает, что по данной цене была отправлена заявка противоположного типа)
    df['buy_volume'] = np.where(df['buy/sell'] == 'B', df['volume'], 0)
    df['sell_volume'] = np.where(df['buy/sell'] == 'S', df['volume'], 0)
    del df['buy/sell']
    del df['volume']
    df.sort_values('price', inplace=True, ascending=False)
    df = df.reindex(columns=['buy_volume', 'price', 'sell_volume'])
    # таблица, визуализирующая стакан
    print(df)

    # оставляем только ненулевые значения
    buy = df.loc[(df['buy_volume'] > 0)]
    sell = df.loc[(df['sell_volume'] > 0)]

    # Создаем  соответствующие списки
    bid_price = buy['price']
    bid_volume = buy['buy_volume']
    ask_price = sell['price']
    ask_volume = sell['sell_volume']

    # добавляем новые значения в списки соответствующих переменных
    mid_price.append(round((float(ask_price[-1:])+float(bid_price[:1]))/2, 3))
    bid_ask_spread.append(round(float(ask_price[-1:])-float(bid_price[:1]), 3))
    depth_ask.append(int(ask_volume[-1:]))
    depth_bid.append(int(bid_volume[:1]))

    # прибавляем к предыдущему моменту времени шаг, заданный пользователем
    time = datetime.strptime(str(time), '%H%M%S%f')
    time = int((time - time_zero + step).time().strftime('%H%M%S%f'))

# создаем список для транзакционных издержек, принимая во внимание, что фактическая длина ряда av_price может быть меньше,
# чем длина ряда mid_price
number = 0
trans_cost = []
for i in range(0,len(mid_price)):
    if av_prices[i]!=0:
        trans_cost.append(abs(mid_price[i]-float(av_prices[i])))
    else:
        number+=1

time_list_new = time_list[number:]


#print(mid_price)
#print(bid_ask_spread)
#print(depth_ask)
#print(depth_bid)
#print(trans_cost)
#print(av_prices)

# Строим графики

fig, ax = plt.subplots()
plt.plot(time_list, mid_price, color='black', label='Mid_price')
plt.plot(time_list, [np.mean(mid_price) for i in range(len(mid_price))], label='Mean', color='green', linestyle='--')
plt.plot(time_list, [np.median(mid_price) for i in range(len(mid_price))], label='Median', color='red', linestyle='--')
plt.plot(time_list, [np.quantile((mid_price), 0.05) for i in range(len(mid_price))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list, [np.quantile((mid_price), 0.95) for i in range(len(mid_price))], label='Quantile 95%', color='orange', linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.plot(time_list, bid_ask_spread, color='black', label='Bid_ask_spread')
plt.plot(time_list, [np.mean(bid_ask_spread) for i in range(len(bid_ask_spread))], label='Mean', color='green', linestyle='--')
plt.plot(time_list, [np.median(bid_ask_spread) for i in range(len(bid_ask_spread))], label='Median', color='red', linestyle='--')
plt.plot(time_list, [np.quantile((bid_ask_spread), 0.05) for i in range(len(bid_ask_spread))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list, [np.quantile((bid_ask_spread), 0.95) for i in range(len(bid_ask_spread))], label='Quantile 95%', color='orange', linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.plot(time_list, depth_ask, color='black', label='Depth_ask')
plt.plot(time_list, [np.mean(depth_ask) for i in range(len(depth_ask))], label='Mean', color='green', linestyle='--')
plt.plot(time_list, [np.median(depth_ask) for i in range(len(depth_ask))], label='Median', color='red', linestyle='--')
plt.plot(time_list, [np.quantile((depth_ask) , 0.05) for i in range(len(depth_ask))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list, [np.quantile((depth_ask), 0.95) for i in range(len(depth_ask))], label='Quantile 95%', color='orange',linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.plot(time_list, depth_bid, color='black', label='Depth_bid')
plt.plot(time_list, [np.mean(depth_bid) for i in range(len(depth_bid))], label='Mean', color='green', linestyle='--')
plt.plot(time_list, [np.median(depth_bid) for i in range(len(depth_bid))], label='Median', color='red', linestyle='--')
plt.plot(time_list, [np.quantile((depth_bid), 0.05)  for i in range(len(depth_bid))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list, [np.quantile((depth_bid), 0.95) for i in range(len(depth_bid))], label='Quantile 95%', color='orange',linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.plot(time_list_new, trans_cost, color='black', label='Depth_bid')
plt.plot(time_list_new, [np.mean(trans_cost) for i in range(len(trans_cost))], label='Mean', color='green', linestyle='--')
plt.plot(time_list_new, [np.median(trans_cost) for i in range(len(trans_cost))], label='Median', color='red', linestyle='--')
plt.plot(time_list_new, [np.quantile((trans_cost), 0.05) for i in range(len(trans_cost))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list_new, [np.quantile((trans_cost), 0.95) for i in range(len(trans_cost))], label='Quantile 95%', color='orange',linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()


fig, ax = plt.subplots()
plt.hist(mid_price, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((mid_price), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Mid_price')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.hist(bid_ask_spread, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((bid_ask_spread), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Bid_ask_spread')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.hist(depth_ask, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((depth_ask), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Depth_ask')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.hist(depth_bid, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((depth_bid), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Depth_bid')
plt.legend()
plt.show()

fig, ax = plt.subplots()
plt.hist(trans_cost, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((trans_cost), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Trans_cost')
plt.legend()
plt.show()



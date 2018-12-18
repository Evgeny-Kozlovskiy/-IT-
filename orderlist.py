import mysql.connector
import decimal


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="6666"
)
cur = conn.cursor()

# использование созданной БД
cur.execute("""USE OrderLog;""")

# исходная БД
data = open('OrderLog20151222.txt').read()
lines = data.split('\n')[1:100000]


"""Запись всех данных в таблицу ordlog"""
# отлов ошибки для возможности повторного запуска скрипта без занесения старых записей в БД
for i, elem in enumerate(lines):
    try:
        t = ''
        for field in elem.split(',')[1:]:
            try:
                float(field)
            except Exception:
                # если данные пустые в исходном тексте, то заменяются 0
                if field == '':
                    t += '0, '
                else:
                    t += f"'{field}', "
            else:
                t += f'{field}, '
        request = f"""INSERT INTO ordlog(seccode, buysell, time, orderno, action, price, volume, tradeno, tradeprice) VALUES ({t[:-2]});"""
        cur.execute(request)
    except Exception as e:
        pass
# применяем изменения
conn.commit()


"""Вытаскиваем все данные"""
cur.execute("""SELECT * from ordlog;""")
rows = cur.fetchall()


"""Проверка корректности записи данных в таблицу ordlog"""
flag = False
for i, record in enumerate(rows):
    source = lines[i].split(',')
    for j, elem in enumerate(record):
        try:
            float(source[j])
        except Exception:
            if elem != source[j]:
                if source[j] == '':
                    continue
                flag = True
                #print('str', elem, source[j])
        else:
            if '.' in source[j]:
                if float(elem) != float(source[j]):
                    flag = True
                    #print('float', elem, source[j], elem == float(source[j]), type(elem), type(float(source[j])))
            else:
                if elem != int(source[j]):
                    flag = True
                    #print('int', elem, source[j], record, source)

print('СОСТОЯНИЕ:  ', 'ОШИБОК НЕТ' if not flag else 'ЕСТЬ ОШИБКИ')


"""Разбивка исходной таблицы на 3 новых"""

# тикеры с сайта биржи
tickers_text = open('ListingSecurityList.csv', encoding = "cp1251").read()

tickers = {}
for elem in tickers_text.split('\n')[:-1]:
    t = elem.split(',')
    tickers[t[7].replace("\"", "")] = t[5].replace("\"", "")

# отлов ошибки для возможности повторного запуска скрипта без занесения старых записей в БД
for record in rows:
    try:
        if tickers[record[1]] == 'Акция привилегированная':
            t = f"""INSERT INTO PreferredStock(seccode, buysell, time, orderno, action, price, volume, tradeno, tradeprice) VALUES {tuple(elem if type(elem) != decimal.Decimal else float(elem) for elem in record[1:])};"""
            cur.execute(t)
        elif tickers[record[1]] == 'Акция обыкновенная':
            t = f"""INSERT INTO CommonStock(seccode, buysell, time, orderno, action, price, volume, tradeno, tradeprice) VALUES {tuple(elem if type(elem) != decimal.Decimal else float(elem) for elem in record[1:])};"""
            cur.execute(t)
        elif 'облигац' in tickers[record[1]].lower():
            t = f"""INSERT INTO Bonds(seccode, buysell, time, orderno, action, price, volume, tradeno, tradeprice) VALUES {tuple(elem if type(elem) != decimal.Decimal else float(elem) for elem in record[1:])};"""
            cur.execute(t)
    except Exception as e:
        pass
conn.commit()


"""Считываем тикеры обыкновенных акций"""
cur.execute("""SELECT * from CommonStock;""")
rows = cur.fetchall()

all_tickers = {}
for r in rows:
    if r[1] in all_tickers:
        all_tickers[r[1]] += 1
    else:
        all_tickers[r[1]] = 0

t = max(all_tickers, key=lambda x: all_tickers[x])
print('Самый частый тикер среди обыкновенных акций: ', t, ', в количестве: ', all_tickers[t])


lista = ['esta vencido', 'te olvido realizar tu pago', "Tu fecha de pago fue ayer", "Paga tu saldo vencido", "Recuerda que al retrasarte arriesgas tu aumento de crédito",
                              "evita un cargo por demora", "Tu fecha de pago fue ayer. Liquida ahora", "no arriesgues tu historial crediticio", "no lastimes tu futuro financiero",
                              "Tu pago se esperaba el"]
lista = [item.lower() for item in lista]

replace = {
    'préstamo': 'prestamo',
    'pr stamo': 'prestamo',
    'crédito': 'credito',
    'cr dito': 'credito',
    'ma}ana': 'mañana'
}

for key,value in replace.items():
    lista = [item.replace(key, value) for item in lista]

lista = sorted(lista)

for item in lista:
    print(f'"{item}",')



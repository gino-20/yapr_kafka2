import faust

# Конфигурация Faust-приложения
app = faust.App(
    "simple-faust-app",
    broker="localhost:9094",
    value_serializer="raw", # Работа с байтами (default: "json")
)

# Определение топика для входных данных
input_topic = app.topic("input-topic", value_type=str)

# Определение топика для выходных данных
output_topic = app.topic("output-topic", value_type=str)

# Функция, реализующая потоковую обработку данных
@app.agent(input_topic)
async def process(stream):
    async for value in stream:
        # Обработка данных
        processed_value = f"Processed: {value}"
        # Отправка обработанных данных в выходной топик
        await output_topic.send(value=processed_value)
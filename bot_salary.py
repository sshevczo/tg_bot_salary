from aiogram import Bot, Dispatcher, types
from main import aggregate_salaries
import json

bot = Bot(token='6775475494:AAFTU_OiXBL2x0PidlfUZuOr_5_IzqBhpMU')
dp = Dispatcher(bot=bot)

def is_valid_json(text):
    try:
        if text:
            json.loads(text)
            return True
    except ValueError:
        return False
    return False

@dp.message_handler(commands=['/start'])
async def get_started(message: types.Message):
    await message.answer('Привет, я бот для агрегации данных о зарплате!')

@dp.message_handler(content_types=types.ContentType.TEXT)
async def procces_json_message(message: types.Message):
    if message.text.startswith('/start'):
        await get_started(message)  # Обработать команду /start
    elif not is_valid_json(message.text):
        await message.answer('Received text is not a valid JSON or it is empty')
        return

    try:
        json_data = json.loads(message.text)
        result = aggregate_salaries(json_data['dt_from'], json_data['dt_upto'], json_data['group_types'])
        await message.answer(result)
    except Exception as ex:
        await message.answer(f'Произошла ошибка: {str(ex)}')

if __name__ == '__main__':
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
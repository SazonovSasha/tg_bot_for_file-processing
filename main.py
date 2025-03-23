import numpy as np
import pandas as pd
import logging
import os
from aiogram import Bot, types
from aiogram.types import InputFile
from aiogram import Application, F

BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
logging.basicConfig(level=logging.INFO)

# Создание объекта Application
app = Application.builder().token(BOT_TOKEN).build()

@app.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    user_full_name = message.from_user.full_name
    logging.info(f'{user_id = } {user_full_name = }')

    await message.reply(f'Привет, {user_full_name}, я телеграмм бот для обработки файла типа .csv! Это может понадобиться для последующего использования например в машинном обучении.')
    await message.reply('Отправь файл типа .csv =)')


@app.message_handler(content_types=['document'])
async def handle_document(message: types.Message):
    if message.document.mime_type == 'text/csv':
        # Скачиваем файл
        file_id = message.document.file_id
        file = await app.bot.get_file(file_id)
        file_path = file.file_path
        # Скачиваем файл на сервер
        file_path_local = f'./{message.document.file_name}'
        await app.bot.download_file(file_path, file_path_local)
        
        try:
            df = pd.read_csv(file_path_local)
            # Удаляем все строки с пустыми ячейками
            df_cleaned = df.dropna()
            
            # Удаляем все дублирующиеся строки
            df_cleaned = df_cleaned.drop_duplicates()

            # Сохраняем обработанный файл обратно
            processed_file_path = './processed_file.csv'

            df_cleaned.to_csv(processed_file_path, index=False)
            # Отправляем обработанный файл пользователю
            await app.bot.send_document(message.chat.id, InputFile(processed_file_path))
            
            # Удаляем локальные файлы после отправки
            os.remove(file_path_local)
            os.remove(processed_file_path)

        except Exception as e:
            logging.error(f"Произошла ошибка при обработке файла: {e}")
            await message.reply(f"Произошла ошибка при обработке файла.")

    else:
        await message.reply("Пожалуйста, отправьте файл в формате .csv.")


if __name__ == '__main__':
    app.run_polling(skip_updates=True)  # Запуск бота с использованием app.run_polling

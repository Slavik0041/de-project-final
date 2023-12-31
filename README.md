### Задача
Выпускной проект
В этом проекте вы поработаете с данными финтех-стартапа, который предлагает международные банковские услуги через приложение: пользователи могут безопасно переводить деньги в разные страны. 
Компания придерживается децентрализованной финансовой системы: в каждой стране, где доступно приложение, есть отдельный сервис, работающий с валютой этой страны. При этом компания ведёт учёт транзакционной активности клиентов внутри и между странами: разработан единый протокол передачи данных, который обеспечивает одинаковую структуру таблиц во всех странах.
Финансовая активность пользователей увеличивается, поэтому пришло время провести единый анализ данных. Вам надо объединить информацию из разных источников и подготовить её для аналитики. 
Описание задачи
Команда аналитиков попросила вас собрать данные по транзакционной активности пользователей и настроить обновление таблицы с курсом валют. 
Цель — понять, как выглядит динамика оборота всей компании и что приводит к его изменениям. 
Данные подготовлены в различных вариантах, так как в компании использовались разные протоколы передачи информации. Поэтому вы можете самостоятельно выбрать источник и решить, каким образом реализовать поставку данных в хранилище.**. 

Входные данные
- **transactions**
  - operation_id — id транзакции;
  - account_number_from — внутренний бухгалтерский номер счёта транзакции ОТ КОГО;
  - account_number_to — внутренний бухгалтерский номер счёта транзакции К КОМУ;
  - currency_code — трёхзначный код валюты страны, из которой идёт транзакция;
  - country — страна-источник транзакции;
  - status — статус проведения транзакции: queued («транзакция в очереди на обработку сервисом»), in_progress («транзакция в обработке»), blocked («транзакция заблокирована сервисом»), done («транзакция выполнена успешно»), chargeback («пользователь осуществил возврат по транзакции»).
  - transaction_type — тип транзакции во внутреннем учёте: authorisation («авторизационная транзакция, подтверждающая наличие счёта пользователя»), sbp_incoming («входящий перевод по системе быстрых платежей»), sbp_outgoing («исходящий перевод по системе быстрых платежей»), transfer_incoming («входящий перевод по счёту»), transfer_outgoing («исходящий перевод по счёту»), c2b_partner_incoming («перевод от юридического лица»), c2b_partner_outgoing («перевод юридическому лицу»).
  - amount — целочисленная сумма транзакции в минимальной единице валюты страны (копейка, цент, куруш);
  - transaction_dt — дата и время исполнения транзакции до миллисекунд.

- сurrencies 
  - date_update — дата обновления курса валют;
  - currency_code — трёхзначный код валюты транзакции;
  - currency_code_with — отношение другой валюты к валюте трёхзначного кода;
  - currency_code_div — значение отношения единицы одной валюты к единице валюты транзакции. 

### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище.
- `/src/sql` - SQL-запросы формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.
- `/src/img` - здесь разместите скриншот реализованного над витриной дашборда.

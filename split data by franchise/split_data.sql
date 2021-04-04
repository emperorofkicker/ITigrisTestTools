-- Удаление данных, не относящихся к текущей франшизе.
-- @franchise_name_input - название текущей франшизы
-- @relations - массив троек; тройки задают цепочку для INNER JOIN-ов по принципу a1.a2 = b1.a3, b1.b2 = c1.b3;
--              сушность к удалению - первый элемент цепочки, a1; произвольная цепочка заканчивается на x1.x2 = departments.id
CREATE OR REPLACE FUNCTION comb(franchise_name_input text, relations text[][])
    RETURNS void AS
$BODY$
DECLARE n int;
        res1 text;
        res2 text := '';
        c1 bigint;
        c2 bigint;

BEGIN
    n = array_length(relations, 1);
    res1 = format('DELETE FROM %s USING ', quote_ident(relations[1][1]));
    FOR i IN 1 .. n
    LOOP
        IF i <> 1 THEN
            res1 = res1 || quote_ident(relations[i][1]) || ', ';
        END IF;
        IF i <> n THEN
            res2 = res2 || format(' AND %s.%s = %s.%s', quote_ident(relations[i][1]), quote_ident(relations[i][2]),
                                                        quote_ident(relations[i + 1][1]), quote_ident(relations[i][3]));
        END IF;
    END LOOP;

    -- Принадлежность к франшизе определяется по департаменту, относящемуся к сущности, по его франшизе
    res1 = res1 || 'departments, franchise WHERE ';
    res2 = res2 || format(' AND %s.%s = departments.id AND departments.franchise = franchise.id AND franchise.name <> %s;',
                                quote_ident(relations[n][1]), quote_ident(relations[n][2]), quote_literal(franchise_name_input));
    res1 = res1 || right(res2, -5);

    -- Не нашел элегантного способа найти кол-во динамически удаляемых строк
    EXECUTE format('SELECT COUNT(*) FROM %s;', relations[1][1]) INTO c1;
    EXECUTE res1;
    EXECUTE format('SELECT COUNT(*) FROM %s;', relations[1][1]) INTO c2;
    RAISE NOTICE '% % deleted, % left', c1 - c2, relations[1][1], c2;
END;$BODY$
    LANGUAGE plpgsql VOLATILE;


-- Удаление данных, не относящихся к текущей франшизе.
-- @franchise_name_input - название текущей франшизы
-- @table_name - отношение, подлежащее чистке
-- Для многих сущностей франшиза определяется через соответствующий им заказ, так что удобно иметь такой метод
CREATE OR REPLACE FUNCTION comb_by_order_id(franchise_name_input text, table_name text)
    RETURNS void AS
$BODY$ 
BEGIN
    EXECUTE comb(franchise_name_input, ARRAY[[table_name, 'order_id', 'id'], ['orders', 'department', '']]);
END;$BODY$
    LANGUAGE plpgsql VOLATILE;


-- Удаление пользователей, не относящихся к данной франшизе; привязка их действий к системному сотруднику (1000000000).
-- @franchise_name_input - название текущей франшизы
-- @table_name_input - отношение, подлежащее чистке
-- @attribute_input (optional) - атрибут @table_name_input, соответствующий id пользователя
-- @condition_input (optional) - дополнительное условие, если есть
CREATE OR REPLACE FUNCTION comb_users_dependencies(franchise_name_input text, table_name_input text,
                                                   attribute_input text DEFAULT 'user_id', condition_input text DEFAULT NULL)
    RETURNS void AS
$BODY$
DECLARE res text;

BEGIN
    res = format('UPDATE %s SET %s = 1000000000 FROM users u, departments d, franchise f WHERE '
                 '%s.%s = u.id AND u.department_id = d.id AND d.franchise = f.id AND f.name <> %s;',
                 table_name_input, attribute_input, table_name_input, attribute_input, quote_literal(franchise_name_input));

    IF condition_input IS NOT NULL THEN
        res = format('%s AND %s;', left(res, -1), condition_input);
    END IF;

    EXECUTE res;
END;$BODY$
    LANGUAGE plpgsql VOLATILE;


-- Очистка прейскурантов. Удаляются правила прейскурантов, не доступные для данного типа франшизы (доступные только для других типов).
-- @franchise_name_input - название текущей франшизы
-- @category_input - категория товара (прейскуранта)
CREATE OR REPLACE FUNCTION comb_pricelists(ftype_input text, category_input text)
    RETURNS void AS
$BODY$
BEGIN
    CREATE TEMPORARY TABLE to_del (
        id bigint
    );

    -- Удалению подлежат правила, доступные набору типов франщиз, не включающему текущий тип франшизы
    EXECUTE format('INSERT INTO to_del '
        'SELECT DISTINCT rule_id AS id FROM franchisestypes_%sprices EXCEPT '
        'SELECT DISTINCT rule_id AS id FROM franchisestypes_%sprices WHERE franchise_type = %s;',
        category_input, category_input, quote_literal(ftype_input));

    EXECUTE format('DELETE FROM franchisestypes_%sprices;', category_input);
    EXECUTE format('DELETE FROM %sprices pl USING to_del WHERE pl.id = to_del.id;', category_input);

    DROP TABLE to_del;
END;$BODY$
    LANGUAGE plpgsql VOLATILE;


-- Основной метод. Требуется удалить данные, не относящиеся к текущей франшизе, а также привести часть данных к формату, совместимым с Optima Complete.
-- В частности, когда очистка данных будет произведена, необходимо удалить франшизы и все их зависимости.
-- @app_name - название нового приложения.
-- @franchise_name_input - название текущей франшизы
CREATE OR REPLACE FUNCTION comb_main(app_name text, franchise_name_input text)
    RETURNS void AS
$BODY$
DECLARE ftype text;
        order_that_remains bigint;
        system_user_id CONSTANT bigint := 1000000000;  -- системный сотрудник
        default_admin_id CONSTANT bigint := 1000000001;  -- дефолтный администратор
        anonymous_client_id CONSTANT bigint := 1000000002;  -- анонимный клиент

BEGIN
    -- Текущий тип франшизы. Удобно иметь под рукой для чистки прейскурантов.
    SELECT type INTO ftype FROM franchise WHERE name = franchise_name_input LIMIT 1;

    -- Чистка прейскурантов
    PERFORM comb_pricelists(ftype, 'accessories');
    PERFORM comb_pricelists(ftype, 'glasses');
    PERFORM comb_pricelists(ftype, 'lenses');
    PERFORM comb_pricelists(ftype, 'sunglasses');

    DELETE FROM franchisestypes_packageordercategories;

    -- Чистка небольших таблиц
    PERFORM comb(franchise_name_input, ARRAY[['access_journal', 'department_id', '']]);

    DELETE FROM cities WHERE city = 'Москва';

    DELETE FROM cldiameters;
    DELETE FROM clradiuses;
    
    PERFORM comb(franchise_name_input, ARRAY[['registry_records_settings', 'department_id', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['versions_browsers', 'department_id', '']]);

    DELETE FROM orderspaymentssubtypes;
    DELETE FROM package_order_categories;
    DELETE FROM suppliers_integration_settings; 
    DELETE FROM wholesale_clients;

    -- Чистка списаний и ревизий
    PERFORM comb(franchise_name_input, ARRAY[['cancellationsgoods', 'cancellation_id', 'id'], ['cancellations', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['cancellations', 'department', '']]);

    PERFORM comb(franchise_name_input, ARRAY[['revisionscomments', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisionsusers', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisionsgoods', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisionsaccessories', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisionsglasses', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisionslenses', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisionssunglasses', 'revision_id', 'id'], ['revisions', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['revisions', 'department', '']]);

    -- Чистка розничных переоценок. Не могло быть переоценок, включающих товары разных франшиз,
    -- так что допустимо определять франшизу переоценки по франшизе департамента товара. В любом случае, другого способа нет.
    CREATE TEMPORARY TABLE to_del AS
    SELECT DISTINCT rg.reeval_id AS id FROM reevaluationgoods rg
    INNER JOIN departments d ON rg.dept_id = d.id
    INNER JOIN franchise f ON d.franchise = f.id
    WHERE f.name <> franchise_name_input;

    PERFORM comb(franchise_name_input, ARRAY[['reevaluationgoods', 'dept_id', '']]);
    DELETE FROM reevaluation r1 USING to_del r2 WHERE r1.id = r2.id;
    DROP TABLE to_del;

    -- Чистка медицинских карт
    PERFORM comb_by_order_id(franchise_name_input, 'medicalchartinstances');

    -- Чистка заявок на товары
    PERFORM comb(franchise_name_input, ARRAY[['orderstosupplierscomments', 'ordertosupplier_id', 'id'], ['orderstosuppliers', 'department_id', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['orderstosuppliersgoods', 'ordertosupplier_id', 'id'], ['orderstosuppliers', 'department_id', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['orderstosuppliers', 'department_id', '']]);

    -- Чистка розничных возвратов
    PERFORM comb(franchise_name_input, ARRAY[['returnedgoods', 'return_id', 'id'], ['returns', 'department', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['servicesreturns', 'return_id', 'id'], ['returns', 'department', '']]);
    PERFORM comb_by_order_id(franchise_name_input, 'returns');

    -- Чистка платежей по заказам
    PERFORM comb_by_order_id(franchise_name_input, 'orderspaymentssubsections');
    PERFORM comb_by_order_id(franchise_name_input, 'orderspaymentssections');
    PERFORM comb_by_order_id(franchise_name_input, 'orderspayments');

    -- Чистка других зависимостей заказов
    PERFORM comb_by_order_id(franchise_name_input, 'orderscomments');

    PERFORM comb_by_order_id(franchise_name_input, 'ordersframes');

    PERFORM comb_by_order_id(franchise_name_input, 'cancelledordersgoods');

    PERFORM comb_by_order_id(franchise_name_input, 'expectedlenses');
    PERFORM comb_by_order_id(franchise_name_input, 'expectedglasses');

    PERFORM comb_by_order_id(franchise_name_input, 'applieddiscounts');

    -- Чистка услуг и типов услуг
    PERFORM comb_by_order_id(franchise_name_input, 'services');

    DELETE FROM servicestypes_groups stg
    USING servicestypes st, franchise f
    WHERE stg.servicetype = st.id AND
          st.franchise_id = f.id AND
          f.name <> franchise_name_input;

    DELETE FROM servicestypes st
    USING franchise f
    WHERE st.franchise_id = f.id AND
          f.name <> franchise_name_input;
    UPDATE servicestypes SET franchise_id = NULL;

    -- Чистка скидок
    DELETE FROM discountsdepartments dd
    USING discounts d, franchise f
    WHERE dd.discount_id = d.id AND
          d.franchise = f.id AND
          f.name <> franchise_name_input;

    DELETE FROM discountsdetalizations dd
    USING discounts d, franchise f
    WHERE dd.common_discount = d.id AND
          d.franchise = f.id AND
          f.name <> franchise_name_input;

    DELETE FROM discounts d
    USING franchise f
    WHERE d.franchise = f.id AND
          f.name <> franchise_name_input;
    UPDATE discounts SET franchise = NULL;

    -- Чистка товаров
    PERFORM comb(franchise_name_input, ARRAY[['accessories', 'id', 'id'], ['goods', 'location', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['glasses', 'id', 'id'], ['goods', 'location', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['lenses', 'id', 'id'], ['goods', 'location', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['sunglasses', 'id', 'id'], ['goods', 'location', '']]);

    PERFORM comb(franchise_name_input, ARRAY[['goods', 'location', '']]);

    -- Чистка заказов и рецептов на очки. Производится параллельно, поскольку заказы и рецепты зависят друг от друга.
    SET CONSTRAINTS fk_orders_prescription DEFERRED;
    PERFORM comb_by_order_id(franchise_name_input, 'prescriptions');
    PERFORM comb(franchise_name_input, ARRAY[['orders', 'department', '']]);
    SET CONSTRAINTS fk_orders_prescription IMMEDIATE;

    -- Чистка поставок
    PERFORM comb(franchise_name_input, ARRAY[['supplies', 'department', '']]);

    -- Чистка клиентов
    DELETE FROM clientscomments cc
    USING clients c, franchise f
    WHERE cc.client_id = c.id AND
          c.franchise = f.id AND
          f.name <> franchise_name_input;

    DELETE FROM clients c
    USING franchise f
    WHERE c.id <> anonymous_client_id AND
          c.franchise = f.id AND
          f.name <> franchise_name_input;
    UPDATE clients SET franchise = NULL;

    -- Чистка респондентов новостей из других франшиз
    PERFORM comb(franchise_name_input, ARRAY[['newsdepartments', 'department_id', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['newsusers', 'user_id', 'id'], ['users', 'department_id', '']]);

    -- Чистка действий пользователей. После чистки не должно остаться пользователей, относящихся к дистрибьютору,
    -- но должно остаться история их операций по заказам и по заявкам на товар от имени системного сотрудника.
    PERFORM comb_users_dependencies(franchise_name_input, 'orderscomments');
    PERFORM comb_users_dependencies(franchise_name_input, 'orderstosupplierscomments');
    PERFORM comb_users_dependencies(franchise_name_input, 'supplies');
    PERFORM comb_users_dependencies(franchise_name_input, 'services', 'employee', 'services.employee IS NOT NULL');

    -- Удаление пользователей других франшиз (за исключением супер-пользователя и админа)
    DELETE FROM users u
    USING departments d, franchise f
    WHERE u.id NOT IN (system_user_id, default_admin_id) AND
          u.department_id = d.id AND
          d.franchise = f.id AND
          f.name <> franchise_name_input;

    UPDATE users SET datasource = app_name;

    -- Чистка настроек департаментов и юридических лиц
    PERFORM comb(franchise_name_input, ARRAY[['departmentssettings', 'department_id', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['legalentitiesdepartments', 'department_id', '']]);
    PERFORM comb(franchise_name_input, ARRAY[['legalentityrules', 'department_id', '']]);

    -- Установка системному сотруднику и дефолтному админу валидного департамента (который не будет удален)
    UPDATE users SET department_id = 
        (SELECT d.id FROM departments d, franchise f WHERE d.franchise = f.id AND f.name = franchise_name_input AND department_type = 'Офис' LIMIT 1)
    WHERE id IN (system_user_id, default_admin_id);

    -- Удаление департаментов, не относящихся к текущей франшизе. Цехи относятся к франшизе дистрибьютора, но их не надо удалять.
    DELETE FROM departments d
    USING franchise f
    WHERE d.department_type <> 'Цех' AND
          d.franchise = f.id AND
          f.name <> franchise_name_input;
    UPDATE departments SET franchise = NULL;

    -- Удаление франшиз (их нет в Optima Complete).
    DELETE FROM franchise;
    DELETE FROM franchisetypes;

    -- Задание совместимого с Optima Complete и актуального контекста
    UPDATE users SET datasource = app_name;

    UPDATE innersettings SET value = 'false' WHERE name = 'hasFranchise';  -- у нас все настройки хранятся как string
    UPDATE innersettings SET value = 'false' WHERE name = 'franchiseTypesMode';
    UPDATE innersettings SET value = 'false' WHERE name = 'franchiseFixPrice';
    UPDATE innersettings SET value = 'false' WHERE name = 'franchiseConsignation';
    UPDATE innersettings SET value = 'false' WHERE name = 'lightVersion1';
    UPDATE innersettings SET value = 'false' WHERE name = 'lightVersion1Depot';
    UPDATE innersettings SET value = 'false' WHERE name = 'lightVersion1Doctor';
    UPDATE innersettings SET value = 'false' WHERE name = 'packageGlassesOrder';
    UPDATE innersettings SET value = 'false' WHERE name = 'onePageOrderClientSelection';
    UPDATE innersettings SET value = 'false' WHERE name = 'onePageOrderExpectedGlassesMode';

    UPDATE users SET can_login_without_certificate = 'true';
    

END;$BODY$
    LANGUAGE plpgsql VOLATILE;


-- Примеры использования
SELECT * FROM comb_main('halleluya', 'Halleluya Abera Eteffa');
SELECT * FROM comb_main('eyecare', 'EYE CARE clinic');
SELECT * FROM comb_main('fua', 'FUA');
-- ...
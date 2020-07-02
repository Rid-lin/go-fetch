# go-quotablock

## Quota Squid Helper

Access Control (ACL) helper for Squid and [Screen Squid](https://sourceforge.net/projects/screen-squid/)

-------------------------------------------------
IMPORTANT: If you are going to use this program and the Quotas module, you need to configure squid.conf.

This lines you need to add to conf:
If your authorization by login:

    #qouta acl section
    external_acl_type e_block ttl=10 negative_ttl=10 %LOGIN /path/to/bin/go_quotablock -typedb mysql -u login -p pass -h host_of_db -n name_of_db -debug 4 -log /var/log/squid/quoteblock.log -ttl 300
    acl a_block external e_block

A shorter record looks like this:

    #qouta acl section
    external_acl_type e_block ttl=10 negative_ttl=10 %LOGIN /path/to/bin/go_quotablock -u login -p pass -n name_of_db
    acl a_block external e_block

Parameters

- typedb mysql
- h localhost
- debug 0
- log /var/log/squid/quoteblock.log
- ttl 300

will be set by default.

If your authorization by IP address:

    #qouta acl section
    external_acl_type e_block ttl=10 negative_ttl=10 %SRC /path/to/bin/go_quotablock -typedb postgres -u login -p pass -h host_of_db -n name_of_db -debug 4 -log /var/log/squid/quoteblock.log -ttl 300
    acl a_block external e_block

## How To Build

If you do not have a golang, please use the [installation instruction](https://golang.org/doc/install).

and next

    git clone https://github.com/Rid-lin/go-quotablock.git
    cd go-quotablock

Edit the makefile.
Please enter valid values for:

    *DIR =*
    *PREFIX =*
    *PROXY-USER =*
    *PROXY-GROUP =*
where *PROXY-USER* and *PROXY-GROUP* is the name of the user and name of the group under which squid runs

    make
    make install

where *squid* is the name of the group under which squid runs

## Thanks

- The idea of implementation was taken from [here](https://github.com/funway/squid-helper) .
- [This material also came in handy](http://freesoftwaremagazine.com/articles/authentication_with_squid/)
- [Without translating directives for Squid, I would need more time](http://break-people.ru/cmsmade/index.php?page=translate_squid_reference_tag_external_acl_type)
- [Very simple log format description](https://wiki.enchtex.info/doc/squidlogformat)

-------------------------------------------------

Quota Squid Helper

Access Control (ACL) helper for Squid и [Screen Squid](https://sourceforge.net/projects/screen-squid/)

-------------------------------------------------
ВАЖНО: Если вы собираетесь использовать данную программу и модуль Quotas из программного проекта [Screen Squid](https://sourceforge.net/projects/screen-squid/), Вам необходимо внести правки в squid.conf.

Эти строки необходимо добавить в конфигурацию

Если авторизация по логину:

    #qouta acl section
    external_acl_type e_block ttl=10 negative_ttl=10 %LOGIN /path/to/bin/go_quotablock [-typedb mysql] -u login -p pass -h host_of_db -n name_of_db [-debug 4] [-log /var/log/squid/quoteblock.log] [-ttl 300]
    acl a_block external e_block

Более короткая запись будет выглядеть вот так:

    #qouta acl section
    external_acl_type e_block ttl=10 negative_ttl=10 %LOGIN /path/to/bin/go_quotablock -u login -p pass -n name_of_db 
    acl a_block external e_block

Параметры

- typedb mysql
- h localhost
- debug 0
- log /var/log/squid/quoteblock.log
- ttl 300

будут подставлены по-умолчанию.

Если авторизация по IP адресу:

    #qouta acl section
    external_acl_type e_block ttl=10 negative_ttl=10 %SRC /path/to/bin/go_quotablock -typedb postgres -u login -p pass -h host_of_db -n name_of_db [-debug 4] [-log /var/log/squid/quoteblock.log] [-ttl 300]
    acl a_block external e_block

## Как установить

Если у Вас не установлен Golang, пожалуйста воспользуйтесь [инструкцией по установке](https://golang.org/doc/install)

и далее

    git clone https://github.com/Rid-lin/go-quotablock.git
    cd go-quotablock

Отредактируйте файл makefile.
Укажите верные значения для:

    *DIR =*
    *PREFIX =*
    *PROXY-USER =*
    *PROXY-GROUP =*
где *PROXY-USER* и *PROXY-GROUP* имя пользователя и группы из-под которого запущен squid

    make
    make install

## Благодарности

- Идея реализации была взята [отсюда](https://github.com/funway/squid-helper)
- Так же пригодился этот [материал](http://freesoftwaremagazine.com/articles/authentication_with_squid/)
- [Без перевода директив для Squid мне понадобилось бы больше времени](http://break-people.ru/cmsmade/index.php?page=translate_squid_reference_tag_external_acl_type)
- [Очень простое описание формата логов](https://wiki.enchtex.info/doc/squidlogformat)

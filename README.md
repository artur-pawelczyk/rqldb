RQLDB is an experimental, purely-relational database, using its own
query language - *RQL*.

# Examples

Define a new relation called "page" with its name as the primary key:

    .define relation page name::TEXT::KEY content::TEXT likes::NUMBER

Insert some data and query it:

    .insert page name = First content = "My first page" likes = 0
    .insert page name = Second content = "Another attempt" likes = 0

    scan page

join_customer_tables = """
    select *
    from customer c inner join person p
    on c.PersonID = p.BusinessEntityID
    
    inner join entityadd ea
    on p.BusinessEntityID = ea.BusinessEntityID

    inner join address add
    on ea.AddressID = add.AddressID

    inner join state s
    on add.StateProvinceID = s.StateProvinceID
"""
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

join_employee_tables = """
    select *
    from employee e inner join depthist dh
    on e.BusinessEntityID = dh.BusinessEntityID

    inner join dept d
    on dh.DepartmentID = d.DepartmentID

    inner join person p
    on e.BusinessEntityID =p.BusinessEntityID

    inner join entityadd ea
    on e.BusinessEntityID = ea.BusinessEntityID

    inner join address add
    on ea.AddressID = add.AddressID

    inner join state s
    on add.StateProvinceID = s.StateProvinceID

"""

join_product_tables ="""
    select *
    from product p left outer join subCat sc
    on p.ProductSubcategoryID = sc.ProductSubcategoryID

    left outer join category c
    on sc.ProductCategoryID = c.ProductCategoryID

    left outer join productHist ph
    on p.productID = ph.productID

    left outer join culture
    on p.ProductModelID = culture.ProductModelID

    left outer join description desc
    on culture.ProductDescriptionID = desc.ProductDescriptionID
"""
join_fact_tables="""
    select soh.*, sod.*
    from SalesOrderHeader soh inner join SalesOrderDetail sod
    on soh.SalesOrderID = sod.SalesOrderID
    
    inner join product p
    on sod.ProductID = p.ProductID
    
    inner join customer c
    on soh.CustomerID = c.CustomerID
    
    left outer join employee
    on soh.SalesPersonID = employee.BusinessEntityID

"""

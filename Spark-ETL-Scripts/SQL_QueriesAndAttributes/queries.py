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
Date_table = """
    select * from bronze.datetable
"""

customer_columns = [
            'CustomerID',
            'CustomerName',
             'AccountNumber',
             'PersonType',
             'NameStyle',
             'Title',
             'Suffix',
             'EmailPromotion',
             'AdditionalContactInfo',
             'Demographics',
             'AddressLine1',
             'AddressLine2',
             'City',
             'PostalCode',
             'SpatialLocation',
             'StateProvinceCode',
             'CountryRegionCode',
             'IsOnlyStateProvinceFlag',
             'stateName'
]



employee_attributes = [
                 'EmployeeID',
                 'EmployeeName',
                 'NationalIDNumber',
                 'LoginID',
                 'OrganizationNode',
                 'OrganizationLevel',
                 'JobTitle',
                 'BirthDate',
                 'MaritalStatus',
                 'Gender',
                 'HireDate',
                 'SalariedFlag',
                 'VacationHours',
                 'SickLeaveHours',
                 'CurrentFlag',
                 'GroupName',
                 'PersonType',
                 'NameStyle',
                 'Title',
                 'Suffix',
                 'EmailPromotion',
                 'AdditionalContactInfo',
                 'Demographics',
                 'AddressLine1',
                 'AddressLine2',
                 'City',
                 'PostalCode',
                 'SpatialLocation',
                 'StateProvinceCode',
                 'CountryRegionCode',
                 'IsOnlyStateProvinceFlag',
                 'stateName'
           
]

product_attributes =[
        'p.ProductID',
         'productName',
         'ProductNumber',
         'MakeFlag',
         'FinishedGoodsFlag',
         'Color',
         'SafetyStockLevel',
         'ReorderPoint',
         'p.StandardCost',
         'ListPrice',
         'Size',
         'SizeUnitMeasureCode',
         'WeightUnitMeasureCode',
         'Weight',
         'DaysToManufacture',
         'ProductLine',
         'Class',
         'Style',
         'SellStartDate',
         'SellEndDate',
         'DiscontinuedDate',
         'subCategoryName',
         'Description'
]

fact_attributes= [
             'soh.SalesOrderID',
             "CustomerKey",
             "EmployeeKey",
             "ProductKey",
             'RevisionNumber',
             'OrderQty',
             'UnitPrice',
             'UnitPriceDiscount',
             'SubTotal',
             'TaxAmt',
             'Freight',
             'TotalDue',
             'OrderDate',
             'DueDate',
             'ShipDate',
             'Status',
             'OnlineOrderFlag',
             'SalesOrderNumber',
             'PurchaseOrderNumber',
             'AccountNumber',
             'CreditCardApprovalCode',
             'Comment',
             'CarrierTrackingNumber',
             'LineTotal'
]


silver_fact_attributes= [
             'SalesOrderID',
             "CustomerKey",
             "EmployeeKey",
             "ProductKey",
             'RevisionNumber',
             'OrderQty',
             'UnitPrice',
             'UnitPriceDiscount',
             'SubTotal',
             'TaxAmt',
             'Freight',
             'TotalDue',
             'OrderDate',
             'DueDate',
             'ShipDate',
             'Status',
             'OnlineOrderFlag',
             'SalesOrderNumber',
             'PurchaseOrderNumber',
             'AccountNumber',
             'CreditCardApprovalCode',
             'Comment',
             'CarrierTrackingNumber',
             'LineTotal'
]

silver_product_attributes =[
        'ProductID',
         'productName',
         'ProductNumber',
         'MakeFlag',
         'FinishedGoodsFlag',
         'Color',
         'SafetyStockLevel',
         'ReorderPoint',
         'StandardCost',
         'ListPrice',
         'Size',
         'SizeUnitMeasureCode',
         'WeightUnitMeasureCode',
         'Weight',
         'DaysToManufacture',
         'ProductLine',
         'Class',
         'Style',
         'SellStartDate',
         'SellEndDate',
         'DiscontinuedDate',
         'subCategoryName',
         'Description'
]
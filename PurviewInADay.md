# PurviewInADay

If you're attending the U2U Purview in a Day, and you want to follow along with the demos, this script provides a few pointers to do so.

## Cloud shell

1. Make sure you enable the Azure cloud shell, since some instructions will make use of this (upper right corner of the Azure portal. This also creates at least one storage account, which will come in handy as well.
2. Create a new container in storage account you already have. You can do this with the following Azure CLI statement from the cloud shell:
```
az storage container create --name "purviewsampledata" --account-name "yourstorageaccountname"
```
3. Copy the `DimCustomer.csv` file from a public storage account (no authentication needed) `https://njlabfiles.blob.core.windows.net/adventureworks/DimCustomer.csv` into the freshly made destination container. This can be done with the following statement (replace with your destination account):
```
az storage blob copy start --source-uri https://njlabfiles.blob.core.windows.net/adventureworks/DimCustomer.csv --account-name yourstorageaccountname --destination-blob DimCustomer.csv --destination-container purviewsampledata
```


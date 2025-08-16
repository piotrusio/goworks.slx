#!/bin/bash

# Start SQL Server in the background
/opt/mssql/bin/sqlservr &

# Store the PID
pid=$!

# Wait for SQL Server to start
echo "Waiting for SQL Server to start..."
for i in {1..60}; do
    if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" -C &> /dev/null; then
        echo "SQL Server is ready"
        break
    fi
    sleep 1
done

# Run init.sql if it exists and hasn't been run before
if [ -f /docker-entrypoint-initdb.d/init.sql ] && [ ! -f /var/opt/mssql/.initialized ]; then
    echo "Running init.sql..."
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /docker-entrypoint-initdb.d/init.sql -C
    touch /var/opt/mssql/.initialized
    echo "init.sql completed"
fi

# Import data if .dat files exist and haven't been imported before
if [ -d /var/opt/mssql/import ] && [ ! -f /var/opt/mssql/.data_imported ]; then
    echo "Checking for data files to import..."
    
    # Count available .dat files
    dat_count=$(find /var/opt/mssql/import -name "*.dat" 2>/dev/null | wc -l)
    
    if [ $dat_count -gt 0 ]; then
        echo "Found $dat_count .dat files. Starting data import..."
        
        # Import Dostawy table
        if [ -f /var/opt/mssql/import/Dostawy.dat ]; then
            echo "Importing Dostawy..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.Dostawy in "/var/opt/mssql/import/Dostawy.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ Dostawy imported successfully"
            else
                echo "✗ Failed to import Dostawy"
            fi
        fi
        
        # Import KntAdresy table
        if [ -f /var/opt/mssql/import/KntAdresy.dat ]; then
            echo "Importing KntAdresy..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntAdresy in "/var/opt/mssql/import/KntAdresy.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntAdresy imported successfully"
            else
                echo "✗ Failed to import KntAdresy"
            fi
        fi
        
        # Import KntGrupyDom table
        if [ -f /var/opt/mssql/import/KntGrupyDom.dat ]; then
            echo "Importing KntGrupyDom..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntGrupyDom in "/var/opt/mssql/import/KntGrupyDom.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntGrupyDom imported successfully"
            else
                echo "✗ Failed to import KntGrupyDom"
            fi
        fi
        
        # Import KntKarty table
        if [ -f /var/opt/mssql/import/KntKarty.dat ]; then
            echo "Importing KntKarty..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntKarty in "/var/opt/mssql/import/KntKarty.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntKarty imported successfully"
            else
                echo "✗ Failed to import KntKarty"
            fi
        fi

        # Import KntLimityK table
        if [ -f /var/opt/mssql/import/KntLimityK.dat ]; then
            echo "Importing KntLimityK..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntLimityK in "/var/opt/mssql/import/KntLimityK.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntLimityK imported successfully"
            else
                echo "✗ Failed to import KntLimityK"
            fi
        fi
        
        # Import KntOpiekun table
        if [ -f /var/opt/mssql/import/KntOpiekun.dat ]; then
            echo "Importing KntOpiekun..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntOpiekun in "/var/opt/mssql/import/KntOpiekun.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntOpiekun imported successfully"
            else
                echo "✗ Failed to import KntOpiekun"
            fi
        fi
        
        # Import KntOsoby table
        if [ -f /var/opt/mssql/import/KntOsoby.dat ]; then
            echo "Importing KntOsoby..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntOsoby in "/var/opt/mssql/import/KntOsoby.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntOsoby imported successfully"
            else
                echo "✗ Failed to import KntOsoby"
            fi
        fi

        # Import KntPromocje table
        if [ -f /var/opt/mssql/import/KntPromocje.dat ]; then
            echo "Importing KntPromocje..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.KntPromocje in "/var/opt/mssql/import/KntPromocje.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ KntPromocje imported successfully"
            else
                echo "✗ Failed to import KntPromocje"
            fi
        fi
        
        # Import Magazyny table
        if [ -f /var/opt/mssql/import/Magazyny.dat ]; then
            echo "Importing Magazyny..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.Magazyny in "/var/opt/mssql/import/Magazyny.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ Magazyny imported successfully"
            else
                echo "✗ Failed to import Magazyny"
            fi
        fi
        
        # Import Nazwy table
        if [ -f /var/opt/mssql/import/Nazwy.dat ]; then
            echo "Importing Nazwy..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.Nazwy in "/var/opt/mssql/import/Nazwy.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ Nazwy imported successfully"
            else
                echo "✗ Failed to import Nazwy"
            fi
        fi
        
        # Import PrcKarty table
        if [ -f /var/opt/mssql/import/PrcKarty.dat ]; then
            echo "Importing PrcKarty..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.PrcKarty in "/var/opt/mssql/import/PrcKarty.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ PrcKarty imported successfully"
            else
                echo "✗ Failed to import PrcKarty"
            fi
        fi

        # Import PrmKarty table
        if [ -f /var/opt/mssql/import/PrmKarty.dat ]; then
            echo "Importing PrmKarty..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.PrmKarty in "/var/opt/mssql/import/PrmKarty.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ PrmKarty imported successfully"
            else
                echo "✗ Failed to import PrmKarty"
            fi
        fi
        
        # Import TwrCeny table
        if [ -f /var/opt/mssql/import/TwrCeny.dat ]; then
            echo "Importing TwrCeny..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TwrCeny in "/var/opt/mssql/import/TwrCeny.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TwrCeny imported successfully"
            else
                echo "✗ Failed to import TwrCeny"
            fi
        fi
        
        # Import TwrCenyNag table
        if [ -f /var/opt/mssql/import/TwrCenyNag.dat ]; then
            echo "Importing TwrCenyNag..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TwrCenyNag in "/var/opt/mssql/import/TwrCenyNag.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TwrCenyNag imported successfully"
            else
                echo "✗ Failed to import TwrCenyNag"
            fi
        fi
        
        # Import TwrGrupyDom table
        if [ -f /var/opt/mssql/import/TwrGrupyDom.dat ]; then
            echo "Importing TwrGrupyDom..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TwrGrupyDom in "/var/opt/mssql/import/TwrGrupyDom.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TwrGrupyDom imported successfully"
            else
                echo "✗ Failed to import TwrGrupyDom"
            fi
        fi
        
        # Import TwrKarty table
        if [ -f /var/opt/mssql/import/TwrKarty.dat ]; then
            echo "Importing TwrKarty..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TwrKarty in "/var/opt/mssql/import/TwrKarty.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TwrKarty imported successfully"
            else
                echo "✗ Failed to import TwrKarty"
            fi
        fi

        # Import TwrPromocje table
        if [ -f /var/opt/mssql/import/TwrPromocje.dat ]; then
            echo "Importing TwrPromocje..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TwrPromocje in "/var/opt/mssql/import/TwrPromocje.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TwrPromocje imported successfully"
            else
                echo "✗ Failed to import TwrPromocje"
            fi
        fi

        # Import TwrZasoby table
        if [ -f /var/opt/mssql/import/TwrZasoby.dat ]; then
            echo "Importing TwrZasoby..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TwrZasoby in "/var/opt/mssql/import/TwrZasoby.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TwrZasoby imported successfully"
            else
                echo "✗ Failed to import TwrZasoby"
            fi
        fi

        # Import ZamNag table
        if [ -f /var/opt/mssql/import/ZamNag.dat ]; then
            echo "Importing ZamNag..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.ZamNag in "/var/opt/mssql/import/ZamNag.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ ZamNag imported successfully"
            else
                echo "✗ Failed to import ZamNag"
            fi
        fi

        # Import ZamElem table
        if [ -f /var/opt/mssql/import/ZamElem.dat ]; then
            echo "Importing ZamElem..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.ZamElem in "/var/opt/mssql/import/ZamElem.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ ZamElem imported successfully"
            else
                echo "✗ Failed to import ZamElem"
            fi
        fi

        # Import TraNag table
        if [ -f /var/opt/mssql/import/TraNag.dat ]; then
            echo "Importing TraNag..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TraNag in "/var/opt/mssql/import/TraNag.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TraNag imported successfully"
            else
                echo "✗ Failed to import TraNag"
            fi
        fi

        # Import TraElem table
        if [ -f /var/opt/mssql/import/TraElem.dat ]; then
            echo "Importing TraElem..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TraElem in "/var/opt/mssql/import/TraElem.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TraElem imported successfully"
            else
                echo "✗ Failed to import TraElem"
            fi
        fi

        # Import TraPlat table
        if [ -f /var/opt/mssql/import/TraPlat.dat ]; then
            echo "Importing TraPlat..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.TraPlat in "/var/opt/mssql/import/TraPlat.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ TraPlat imported successfully"
            else
                echo "✗ Failed to import TraPlat"
            fi
        fi

        # Import Obiekty table
        if [ -f /var/opt/mssql/import/Obiekty.dat ]; then
            echo "Importing Obiekty..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.Obiekty in "/var/opt/mssql/import/Obiekty.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ Obiekty imported successfully"
            else
                echo "✗ Failed to import Obiekty"
            fi
        fi

        # Import BilansOtwarciaElem table
        if [ -f /var/opt/mssql/import/BilansOtwarciaElem.dat ]; then
            echo "Importing BilansOtwarciaElem..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.BilansOtwarciaElem in "/var/opt/mssql/import/BilansOtwarciaElem.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ BilansOtwarciaElem imported successfully"
            else
                echo "✗ Failed to import BilansOtwarciaElem"
            fi
        fi

        # Import BilansOtwarciaSElem table
        if [ -f /var/opt/mssql/import/BilansOtwarciaSElem.dat ]; then
            echo "Importing BilansOtwarciaSElem..."
            /opt/mssql-tools18/bin/bcp ERPXL_GO.CDN.BilansOtwarciaSElem in "/var/opt/mssql/import/BilansOtwarciaSElem.dat" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u
            if [ $? -eq 0 ]; then
                echo "✓ BilansOtwarciaSElem imported successfully"
            else
                echo "✗ Failed to import BilansOtwarciaSElem"
            fi
        fi

        # Mark import as completed
        touch /var/opt/mssql/.data_imported
        echo "Data import process completed"
        
        # Show summary of imported data
        echo "========================================="
        echo "Import Summary:"
        echo "========================================="
        
        # Try to get row counts, but don't fail if SSL issues persist
        if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1" -C &> /dev/null; then
            /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -Q "
            SELECT 'Dostawy' as TableName, COUNT(*) as qty FROM ERPXL_GO.CDN.Dostawy
            UNION ALL SELECT 'KntAdresy', COUNT(*) FROM ERPXL_GO.CDN.KntAdresy
            UNION ALL SELECT 'KntGrupyDom', COUNT(*) FROM ERPXL_GO.CDN.KntGrupyDom
            UNION ALL SELECT 'KntKarty', COUNT(*) FROM ERPXL_GO.CDN.KntKarty
            UNION ALL SELECT 'KntLimityK', COUNT(*) FROM ERPXL_GO.CDN.KntLimityK
            UNION ALL SELECT 'KntOpiekun', COUNT(*) FROM ERPXL_GO.CDN.KntOpiekun
            UNION ALL SELECT 'KntOsoby', COUNT(*) FROM ERPXL_GO.CDN.KntOsoby
            UNION ALL SELECT 'KntPromocje', COUNT(*) FROM ERPXL_GO.CDN.KntPromocje
            UNION ALL SELECT 'Magazyny', COUNT(*) FROM ERPXL_GO.CDN.Magazyny
            UNION ALL SELECT 'Nazwy', COUNT(*) FROM ERPXL_GO.CDN.Nazwy
            UNION ALL SELECT 'PrcKarty', COUNT(*) FROM ERPXL_GO.CDN.PrcKarty
            UNION ALL SELECT 'PrmKarty', COUNT(*) FROM ERPXL_GO.CDN.PrmKarty
            UNION ALL SELECT 'TwrCeny', COUNT(*) FROM ERPXL_GO.CDN.TwrCeny
            UNION ALL SELECT 'TwrCenyNag', COUNT(*) FROM ERPXL_GO.CDN.TwrCenyNag
            UNION ALL SELECT 'TwrGrupyDom', COUNT(*) FROM ERPXL_GO.CDN.TwrGrupyDom
            UNION ALL SELECT 'TwrKarty', COUNT(*) FROM ERPXL_GO.CDN.TwrKarty
            UNION ALL SELECT 'TwrPromocje', COUNT(*) FROM ERPXL_GO.CDN.TwrPromocje
            UNION ALL SELECT 'TwrZasoby', COUNT(*) FROM ERPXL_GO.CDN.TwrZasoby
            UNION ALL SELECT 'ZamElem', COUNT(*) FROM ERPXL_GO.CDN.ZamElem
            UNION ALL SELECT 'ZamNag', COUNT(*) FROM ERPXL_GO.CDN.ZamNag
            UNION ALL SELECT 'TraNag', COUNT(*) FROM ERPXL_GO.CDN.TraNag
            UNION ALL SELECT 'TraElem', COUNT(*) FROM ERPXL_GO.CDN.TraElem
            UNION ALL SELECT 'TraPlat', COUNT(*) FROM ERPXL_GO.CDN.TraPlat
            UNION ALL SELECT 'Obiekty', COUNT(*) FROM ERPXL_GO.CDN.Obiekty
            UNION ALL SELECT 'BilansOtwarciaElem', COUNT(*) FROM ERPXL_GO.CDN.BilansOtwarciaElem
            UNION ALL SELECT 'BilansOtwarciaSElem', COUNT(*) FROM ERPXL_GO.CDN.BilansOtwarciaSElem
            " -C
        else
            echo "Unable to connect for summary (SSL issues), but data import completed successfully"
            echo "You can manually check table counts after container is fully started"
        fi
        echo "========================================="
        
    else
        echo "No .dat files found in /var/opt/mssql/import/"
    fi
else
    if [ -f /var/opt/mssql/.data_imported ]; then
        echo "Data has already been imported (found .data_imported marker)"
    fi
    if [ ! -d /var/opt/mssql/import ]; then
        echo "Import directory /var/opt/mssql/import does not exist"
    fi
fi

if [ -f /docker-entrypoint-initdb.d/post-import.sql ] && [ ! -f /var/opt/mssql/.post_import_completed ]; then
	echo "Running post-import.sql..."
	/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /docker-entrypoint-initdb.d/post-import.sql -C
	if [ $? -eq 0 ]; then
		touch /var/opt/mssql/.post_import_completed
		echo "✓ post-import.sql completed successfully"
	else
		echo "✗ Failed to run post-import.sql"
	fi
fi

# Wait for SQL Server process
wait $pid
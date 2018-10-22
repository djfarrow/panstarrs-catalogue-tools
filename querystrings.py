TEST_QUERY = "select top 10 * from StackObjectThin into mydb.[{table_name}]"
TEST_QUERY_NODB = "select top 10 * from StackObjectThin"

QUERY_STRING_SDSS = \
    """
       -- Select an SDSS comparison sample
       SELECT objID, ra, dec, type, psfMag_g, psfMag_r, psfMag_i, petroMag_g, petroMag_r, petroMag_i, g, r, i,
       flags_g, flags_r, flags_i 
       INTO mydb.{table_name} from PhotoPrimary  
       WHERE ra BETWEEN {raLow} AND {raHigh}
       AND dec BETWEEN {decLow} AND {decHigh}
    """

QUERY_STRING_PS1 = \
     """
        -- Select a bright catalogue of objects 
        -- which are probably real
    
        select st.uniquePspsSTid, st.objID, ira, idec,
        gKronMag, gKronMagErr,
        rKronMag, rKronMagErr,
        iKronMag, iKronMagErr,
        gPSFMag, gPSFMagErr,
        rPSFMag, rPSFMagErr,
        iPSFMag,  iPSFMagErr,
        ginfoFlag, ginfoFlag2, ginfoFlag3,
        rinfoFlag, rinfoFlag2, rinfoFlag3,
        iinfoFlag, iinfoFlag2, iinfoFlag3,
    
        -- Stuff for s/g separation if needed
        sa.ipsfMajorFWHM,
        sa.ipsfMinorFWHM,
        sa.iKronRad
        INTO mydb.[{table_name}] FROM stackObjectThin as st
        JOIN StackObjectAttributes as sa on st.uniquePspsSTid=sa.uniquePspsSTid
        WHERE iKronMag < 25.0
    
        -- Require detections in three bands
        -- AND gKronMag > 0.0
        -- AND rKronMag > 0.0
        -- AND iKronMag > 0.0
    
        -- Location cut
        AND ira BETWEEN {raLow} AND {raHigh}
        AND idec BETWEEN {decLow} AND {decHigh}
    
        -- Deal with overlaps
        AND st.primaryDetection = 1;
    """

QUERY_STRING_PS1_VIEW = \
     """
        -- Select a bright catalogue of objects 
        -- which are probably real
    
        select uniquePspsSTid, objID, ira, idec,
        gKronMag, gKronMagErr,
        rKronMag, rKronMagErr,
        iKronMag, iKronMagErr,
        gPSFMag, gPSFMagErr,
        rPSFMag, rPSFMagErr,
        iPSFMag,  iPSFMagErr,
        ginfoFlag, ginfoFlag2, ginfoFlag3,
        rinfoFlag, rinfoFlag2, rinfoFlag3,
        iinfoFlag, iinfoFlag2, iinfoFlag3,
        
        -- Galactic latitude
        b,
    
        -- Stuff for s/g separation if needed
        ipsfMajorFWHM,
        ipsfMinorFWHM,
        iKronRad
        INTO mydb.[{table_name}] FROM stackObjectView
        WHERE iKronMag < 25.0
    
        -- Require detections in two bands
        AND rKronMag > 0.0
        AND iKronMag > 0.0

        -- Remove low galactic latitude sources
        AND ABS(b) > 20.0    

        -- Location cut
        AND ira BETWEEN {raLow} AND {raHigh}
        AND idec BETWEEN {decLow} AND {decHigh}
    
        -- Deal with overlaps
        AND primaryDetection = 1;
    """

QUERY_STRING_PS1_VIEW_NODB = \
     """
        -- Select a bright catalogue of objects 
        -- which are probably real
    
        select uniquePspsSTid, objID, ira, idec,
        gKronMag, gKronMagErr,
        rKronMag, rKronMagErr,
        iKronMag, iKronMagErr,
        gPSFMag, gPSFMagErr,
        rPSFMag, rPSFMagErr,
        iPSFMag,  iPSFMagErr,
        ginfoFlag, ginfoFlag2, ginfoFlag3,
        rinfoFlag, rinfoFlag2, rinfoFlag3,
        iinfoFlag, iinfoFlag2, iinfoFlag3,
        
        -- Galactic latitude
        b,
    
        -- Stuff for s/g separation if needed
        ipsfMajorFWHM,
        ipsfMinorFWHM,
        iKronRad
        FROM stackObjectView
        WHERE iKronMag < 25.0
    
        -- Require detections in two bands
        AND rKronMag > 0.0
        AND iKronMag > 0.0

        -- Remove low galactic latitude sources
        AND ABS(b) > 10.0    

        -- Location cut
        AND ira BETWEEN {raLow} AND {raHigh}
        AND idec BETWEEN {decLow} AND {decHigh}
    
        -- Deal with overlaps
        AND primaryDetection = 1;
    """




QUERY_STRING_PS1_STACKTHIN = \
     """
        -- Select a bright catalogue of objects 
        -- which are probably real
    
        select uniquePspsSTid, objID, ira, idec,
        gKronMag, gKronMagErr,
        rKronMag, rKronMagErr,
        iKronMag, iKronMagErr,
        iPSFMag,  iPSFMagErr,
        ginfoFlag, ginfoFlag2, ginfoFlag3,
        rinfoFlag, rinfoFlag2, rinfoFlag3,
        iinfoFlag, iinfoFlag2, iinfoFlag3
    
        -- Stuff for s/g separation if needed
        INTO mydb.[{table_name}] FROM stackObjectThin
        WHERE iKronMag < 24.0
    
        -- Require detections in three bands
        AND gKronMag > 0.0
        AND rKronMag > 0.0
        AND iKronMag > 0.0
    
        -- Location cut
        AND ira BETWEEN {raLow} AND {raHigh}
        AND idec BETWEEN {decLow} AND {decHigh}
    
        -- Deal with overlaps
        AND primaryDetection = 1;
    """




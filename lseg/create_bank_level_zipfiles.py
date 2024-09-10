'''
Purpose: Reorganize PDF files in a 7z folder into new ZIP folder based on year and company name, 
         and normalize file names into lower case.
Author: Oliver Wang
Date: May 14, 2024
Details: This script processes .7z folder containing PDFs, htm, and xlsx files in nested 
    directories. It categorizes these files into new ZIP folder named after the company and 
    stores them in sub-folders named after the year. It uses py7zr for handling 7z folders 
    and Python's standard zipfile library for creating new ZIP files.
'''

# %% ===========================================================================
# Import necessary libraries
# ==============================================================================

import os
import zipfile
import re
import py7zr

# %% ===========================================================================
# Main wrapper function
# ==============================================================================

def main():
    """
    Main function that reorganizes files from a 7z archive into new ZIP archives based on year and company name,
    and normalizes file names.
    """
    input_7z_path = 'datastore/raw/reports/lseg/orig/zipped/Analyst report downloads.7z'
    output_folder = 'datastore/raw/reports/lseg/orig/bank_zip'
    # Call the function to extract and reorganize content from the 7z archive.

    # The pattern is used to extract the year and company name from the file name,
    # The file name is expected to be in the format of 'YYYY-MM-DD-Company Name-...'
    # For example, '2004-08-03-abg_sundal_collier_(-swedish_equity_strategy_-_hold_-_as_good_as_it_gets-33283210.pdf'
    file_pattern = re.compile(r'^(\d{4})-\d{2}-\d{2}-(.+?)-.*\.(pdf|htm|xlsx)$', re.I)

    new_zips = {}
    # Create a temporary directory to extract files from the 7z archive.
    temp_extracted_path = os.path.join(os.getcwd(), "temporary_extracted_files")
    # Create the temporary directory if it does not exist.
    with py7zr.SevenZipFile(input_7z_path, 'r') as zipf:
        zipf.extractall(path=temp_extracted_path) # Extract the contents of the 7z archive to the temporary directory.
        categorize_directories(temp_extracted_path, file_pattern, new_zips, output_folder)

    # Get manual adjustments to the zip folder
    rename_map, old_filenames, new_filenames = do_manual_adjustments(output_folder)

    for old_name, new_name in rename_map.items():
        os.rename(os.path.join(output_folder, old_name), os.path.join(output_folder, new_name))

    for old_name, new_name in zip(old_filenames, new_filenames):
        old_path = os.path.join(output_folder, old_name)
        new_path = os.path.join(output_folder, new_name)
        os.rename(old_path, new_path)
    # Close all new zip files and clean up
    for zip_file in new_zips.values():
        zip_file.close()
    # Remove the temporary directory and its contents.
    for root, dirs, files in os.walk(temp_extracted_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))

# ==============================================================================
# Define functions for processing the steps of the reorganization of directories
# ==============================================================================

def categorize_directories(current_directory, file_pattern, new_zips, output_folder):
    """
    Recursively process directories and categorize files based on the file pattern.

    Args:
    current_directory (str): The current directory to process.
    file_pattern (re.Pattern): The regex pattern to match against file names.
    new_zips (dict): A dictionary of ZIP files to which files will be added.
    output_folder (str): The output folder where the new ZIP files will be created.
    """
    # Iterate over the files and subdirectories in the current directory.
    for analyst_reports in os.listdir(current_directory):
        # Create the path to the file or subdirectory.
        analyst_reports_path = os.path.join(current_directory, analyst_reports)
        # Check if the path is a directory or a file.
        if os.path.isdir(analyst_reports_path):
            categorize_directories(analyst_reports_path, file_pattern, new_zips, output_folder)
        else:
            reorganize_file(analyst_reports_path, file_pattern, new_zips, output_folder)

# ==============================================================================
# Define functions for reorganizing individual files
# ==============================================================================
def reorganize_file(file_path, file_pattern, new_zips, output_folder):
    """
    Process individual analyst reports files, matching against a regex pattern, 
    and archive them in ZIP based on company and year calling the function of 
    adding files to the corresponding ZIP file.

    Args:
    file_path (str): The path to the file to process.
    file_pattern (re.Pattern): The regex pattern to match against file names.
    new_zips (dict): A dictionary of ZIP files to which files will be added.
    output_folder (str): The output folder where the new ZIP files will be created.
    """
    # Extract the year and company name from the file name using the regex pattern.
    reports_name = os.path.basename(file_path)
    match = file_pattern.match(reports_name)
    if match:
        # Extract the year and company name from the match object.
        year, company, _ = match.groups()
        # Correct the company and year names to lowercase with underscores.
        company = lowercase_and_replace_spaces(company)
        year = lowercase_and_replace_spaces(year)
        # Correct the reports name to lowercase with underscores.
        reports_name = lowercase_and_replace_spaces(reports_name)
        # Create the new ZIP file name based on the company name.
        new_zip_name = f"{company}.zip"
        new_zip_path = os.path.join(output_folder, new_zip_name)
        # Create the internal path within the ZIP archive.
        internal_path = os.path.join(year, reports_name)
        # Call the function to add the file to the corresponding ZIP file.
        add_file_to_zip(file_path, new_zip_name, internal_path, new_zips)
    else:
        print(f"{reports_name} does not match the expected pattern. Skipping.")

# ==============================================================================
# Define the function to add files to the corresponding ZIP file
# ==============================================================================

def add_file_to_zip(file_path, new_zip_name, internal_path, new_zips):
    """
    Add the file to the corresponding ZIP file if it doesn't already exist there,
    and print a message indicating the addition.

    Args:
    file_path (str): The path to the file to add to the ZIP archive.
    new_zip_name (str): The name of the ZIP archive to add the file to.
    internal_path (str): The internal path within the ZIP archive to store the file.
    new_zips (dict): A dictionary of ZIP files to which files will be added.
    """
    # Check if the ZIP file is already open or needs to be opened.
    if new_zip_name not in new_zips:
        new_zips[new_zip_name] = zipfile.ZipFile(new_zip_name, 'a', zipfile.ZIP_DEFLATED)
    # Check if the analyst reports is already in the ZIP archive.
    if internal_path not in new_zips[new_zip_name].namelist():
        # Add the analyst reports to the ZIP archive under the internal path.
        with open(file_path, 'rb') as file_data:
            new_zips[new_zip_name].writestr(internal_path, file_data.read())
            print(f"Added {os.path.basename(file_path)} to {new_zip_name} under {internal_path}")
    else:
        print(f"{os.path.basename(file_path)} already exists in {new_zip_name}. Skipping.")
# ==============================================================================
# Define the function to lowercase and replace spaces with underscores in names
# ==============================================================================

def lowercase_and_replace_spaces(name: str) -> str:
    """
    Correct file and directory names to lowercase with underscores instead of spaces.
    """
    return name.lower().replace(' ', '_')

# ==============================================================================
# Do manual adjustments to the zip folder
# ==============================================================================

def do_manual_adjustments(output_folder: str) -> tuple[list, list, list]:
    '''
    This part is to simulate the manual adjustment of the zip folder due to the file name not in the expected format.
    First, some files need to be renamed to match the expected format and be simpler.
    Second, some zip files need to be merged into a single zip file for the same investment bank.
    '''
    
    rename_map = {
        'actinver_casa_de_bolsa.zip': 'actinver.zip',
        'scvb.st^f14.zip': 'alandsbanken.zip',
        'alpha_finance_sa.zip': 'alpha_finance.zip',
        'ambit_capital_pvt_ltd.zip': 'ambit_capital.zip',
        'aton_llc.zip': 'aton.zip',
        'attijari_intermediat.zip': 'attijari.zip',
        'audi_saradar_investm.zip': 'audi_capital.zip',
        'analisis_banco_sabad.zip': 'bancsabadell.zip',
        'bear_stearns__co._i.zip': 'bear_stearns.zip',
        'bmo_capital_markets.zip': 'bmo.zip',
        'bulltick,_llc.zip': 'bulltick.zip',
        'esngvc_gaesco_beka,.zip': 'caja_madrid_bolsa.zip',
        'capital_alliance_sec.zip': 'capital_alliance.zip',
        'cardinal_stone_partn.zip': 'cardinal_stone.zip',
        'ccb_international_se.zip': 'ccb_international.zip',
        'cibc_world_markets.zip': 'cibc.zip',
        'clarksons_platou_sec.zip': 'clarksons_platou.zip',
        '0603.hk.zip': 'cms.zip',
        'collins_stewart_euro.zip': 'collins_stewart.zip',
        'concorde_capital_lim.zip': 'concorde.zip',
        'mirae_asset_daewoo.zip': 'daewoo.zip',
        'databank_group.zip': 'databank.zip',
        'deutsche_bank_equity.zip': 'deutsche_bank.zip',
        'edelweiss_capital_li.zip': 'edelweiss_capital.zip',
        'elara_securities_pvt.zip': 'elara_securities.zip',
        'eurobank_securities_.zip': 'eurobank.zip',
        'evercore_isi.zip': 'evercore.zip',
        'fisrt_capital_equiti.zip': 'fisrt_capital.zip',
        'garanti_bbva_securit.zip': 'garanti_bbva.zip',
        'gbm_casa_de_bolsa.zip': 'gbm.zip',
        'global_investment_ho.zip': 'global_investment.zip',
        'hdfc_securities_inst.zip': 'hdfc.zip',
        'icbc_international_s.zip': 'icbc_international.zip',
        'imara_s.p._reid_pty_.zip': 'imara.zip',
        'pt_indo_premier_secu.zip': 'indo_premier.zip',
        'intesa)_sanpaolo_equi.zip': 'intesa_sanpaolo.zip',
        'investec_bank_(uk)_p.zip': 'investec_bank_(uk).zip',
        'caci.n.zip': 'jefferies.zip',
        'alandsbanken.zip': 'kaupthing.zip',
        'kt_zmico_securities.zip': 'kt_zmico.zip',
        'kepler_cheuvreux.zip': 'landsbanki.zip',
        'larrain_vial_s.a._co.zip': 'larrain.zip',
        'loewen,_ondaatje,_mc.zip': 'lom.zip',
        'macquarie_research.zip': 'macquarie.zip',
        'mf_global_fxa_securi.zip': 'mf_global.zip',
        'national_bank_financ.zip': 'national_bank.zip',
        'alvg.de.zip': 'natixis_bleichroeder.zip',
        'natwest_securities_c.zip': 'natwest.zip',
        'fim.zip': 'nordic.zip',
        'orient_securities_co.zip': 'orient_securities.zip',
        'osk_research_sdn_bhd.zip': 'osk_research.zip',
        'paine_webber_incorpo.zip': 'paine_webber.zip',
        'pareto_securities_as.zip': 'pareto.zip',
        'penser_access_(commi.zip': 'penser.zip',
        'philip_securities_r.zip': 'philip_capital.zip',
        'pnc_instituitional_in.zip': 'pnc.zip',
        'prabhudas_lilladher_.zip': 'prabhudas_lilladher.zip',
        'pt_danareksa_sekurit.zip': 'pt_danareksa.zip',
        'raiffeisen_research_.zip': 'raiffeisen.zip',
        'raymond_james_securi.zip': 'raymond_james.zip',
        'rbs_(desk_strategy).zip': 'rbs.zip',
        'rencap_securities_(p.zip': 'rencap_securities.zip',
        '2384.t.zip': 'sacombank_securities.zip',
        'safra_research.zip': 'safra.zip',
        '1114.hk.zip': 'sbi_china_capital.zip',
        'seaport_global_secur.zip': 'seaport_global.zip',
        'shinhan_investment_c.zip': 'shinhan_investment.zip',
        'shore_capital_stockb.zip': 'shore_capital.zip',
        'international_securi.zip': 'shuaa_capital.zip',
        'sinopac_sec_investme.zip': 'sinopac_sec.zip',
        'taib_securities_w.l..zip': 'taib_securities.zip',
        'pt_trimegah_sekurita.zip': 'trimegah.zip',
        'united_capital_plc.zip': 'united_capital.zip',
        'united_securities_ll.zip': 'united_securities.zip',
        'unlu__co.zip': 'unlu.zip',
        'wells_fargo_securit.zip': 'wells_fargo.zip',
        'wedbush_securities_i.zip': 'wedbush_securities.zip',
        'yatirim_finansman_se.zip': 'yatirim_finansman.zip',
        'yuanta_securities_(k.zip': 'yuanta_securities.zip'}

    # Merging the zip files into a single zip file for the same investment bank
    old_filenames = ['abg_sundal_collier_(.zip','triba.kl^b02.zip','misc.kl.zip','0678.hk.zip',
                     'triba.kl^b02.zip','bato.kl.zip','maly.kl.zip','mlto.kl.zip',
                     'tsrp.kl.zip','wlow.kl.zip','ybve.kl.zip','okac.kl.zip',
                     'etex.kl.zip','irib.kl.zip','misc.kl.zip','trcg.kl.zip',
                     'tlmm.kl.zip','amwa.kl.zip','cimb.kl.zip','oter.at.zip',
                     'stck.l^k21.zip','opar.at.zip','nas.ol.zip','tme.nz^e19.zip',
                     '1102.tw.zip','nkt.co.zip','cey.l.zip','nbgr.at.zip',
                     'bbl.bk.zip','ttnr.at^i19.zip','akbnk.is.zip','msich.pft.zip',
                     'advanc.bk.zip','top.bk.zip','fib.bb.zip','elc_baillieu.zip',
                     'barclays_de_zoete_we.zip','infy.ns.zip','jnsp.ns.zip','3673.tw.zip',
                     'glen.ns.zip','delb.br^g16.zip','mt.as.zip','sgef.pa.zip',
                     'ror.l.zip','msft.oq.zip','baer.s.zip','bnpp.pa.zip',
                     'mro.n.zip','emr.n.zip','oxy.n.zip','bdev.l.zip',
                     'sapg.de.zip','lnc.n.zip','0017.hk.zip','ifxgn.de.zip',
                     'bmwg.de.zip','bkng.oq.zip','inga.as.zip','f.n.zip',
                     'mt.as.zip','vrsk.oq.zip','kss.n.zip','axaf.pa.zip',
                     'amgn.oq.zip','rya.i.zip','ess.n.zip','carr.pa.zip',
                     'sgro.l.zip','wmt.n.zip','nxpi.oq.zip','rog.s.zip',
                     'dida.mc.zip','gww.n.zip','pru.l.zip','adn.l^h17.zip',
                     'icad.pa.zip','iii.l.zip','svt.l.zip','bp.l.zip',
                     'agrn.si^h11.zip','2311.tw.zip','0001.hk.zip','bni.sn^i18.zip',
                     'heli.ca.zip','ecl.sn.zip','bnp_paribas_fortis_(.zip','exane_bnp_paribas.zip',
                     'bnp_paribas_exane.zip','mypk3.sa.zip','aleatic.mx.zip','labb.mx.zip',
                     'vale3.sa.zip','klbn4.sa.zip','sun.ns.zip','005930.zip'
                     'funo11.mx.zip','sber.mm.zip','032830.ks.zip','600276.ss.zip',
                     '601318.ss.zip','0388.hk.zip','005380.ks.zip','amzn.oq.zip',
                     '005830.ks.zip','antm.jk.zip','reli.ns.zip','000858.sz.zip',
                     '051910.ks.zip','credit_suisse_.zip','boot.oq.zip','vod.l.zip',
                     'mtel.bu.zip','ykbnk.is.zip','molb.bu.zip','kgf.l.zip',
                     'accp.pa.zip','lloy.l.zip','aal.l.zip','abpa.l^i06.zip',
                     'bmwg.de.zip','alyzf.pk^h09.zip','muvgn.de.zip','bmwg.de.zip',
                     'mmm.n.zip','bimas.is.zip','ords.qa.zip','hdbk.ns.zip',
                     'hsba.l.zip','600362.ss.zip','abi.br.zip','sprm/si^e22.zip',
                     'asii.jk.zip','0016.hk.zip','infy.ns.zip','wbsv.vi.zip',
                     'akbnk.is.zip','adcb.ad.zip','aal.l.zip','enrc.l^k13.zip',
                     'cnr.to.zip','bats.l.zip','zain.kw.zip','2412.tw.zip',
                     'cop.n.zip','3020.se.zip','2330.se.zip','2330.tw.zip',
                     'xta.l^e13.zip','pot.to^a18.zip','1120.se.zip','bt.l.zip',
                     'fees.mm.zip','000270.ks.zip','mlc.l^j19.zip',
                     'ry.to.zip','pgn.wa^k22.zip','012330.ks.zip','infy.ns.zip',
                     '005380.ks.zip','adbn.s^c15.zip','bidu.oq.zip','sedu3.sa.zip',
                     'catl.si^i21.zip','0016.hk.zip','aap.n.zip','slhn.s.zip',
                     '1928.hk.zip','cpall.bk.zip','alpeka.mx.zip','alvg.de.zip',
                     '066570.ks.zip','601898.ss.zip','005930.ks.zip','051990.ks.zip',
                     'ofcb.mm^l17.zip','ara.mx.zip','pgs.ol.zip','1010.se.zip',
                     '000528.sz.zip','0003.hk.zip','gmexicob.mx.zip','1044.hk.zip',
                     'axlj.j^f20.zip','cco.to.zip','cbkg.de.zip','rya.i.zip',
                     'giaa.jk.zip','eregl.is.zip','0151.hk.zip','bbri.jk.zip',
                     'dbs_vickers_research.zip','becl.bk^l15.zip','0013.hk^f15',
                     '0001hk^c15.zip','becl.bk^l15.zip','hkld.si.zip','dbs_vickers',
                     'bbri.jk.zip','hlbb.kl.zip','mone.si^d19.zip','asth.wa.zip',
                     'esncic_market_solut.zip','esncm.zip','pgs.ol.zip','na.to.zip',
                     'hlbr.at^g13.zip','oter.at.zip','excr.at.zip','ttnr.at^i19.zip',
                     'ko.n.zip','clari.pa.zip','telmexl.mx^l.zip','bkt.mc.zip',
                     'gomo.mx.zip','bbas3.sa.zip','ciel3.sa.zip','mypk3.sa.zip',
                     'hype3.sa.zip','omab.mx.zip','agro.n.zip','cencosud.sn.zip',
                     'enbr3.sa.zip','bbdc4.sa.zip','sqma.sn.zip','vale3.sa.zip',
                     'wege3.sa.zip','rdor3.sa.zip','abev3.sa.zip','ht1.ax.zip',
                     'osg.n.zip','chk.oq.zip','wrk.n.zip','plxs.oq.zip',
                     'f.n.zip','tap.n.zip','biib.oq.zip','mhk.n.zip',
                     'valpq.pk^d21.zip','orcl.n.zip','ocsl.oq.zip','sbux.oq.zip',
                     'ctsh.oq.zip','irm.n.zip','cadbury.lg.zip','pep.oq.zip',
                     'rnwk.oq^l22.zip','nktr.oq.zip','onxx.oq^j13.zip','ko.n.zip',
                     'shfl.oq^k13.zip','cpg.l.zip','bg.l^b16.zip','dsv.co.zip',
                     'top.bk.zip','kbank.bk.zip','cba.ax.zip','coh.ax.zip',
                     'cxp.ax^i10.zip','rio.ax.zip','srs.ax^g19.zip','rmd.n.zip',
                     'tcs.ns.zip','asok.ns.zip','brti.ns.zip','mahm.ns.zip',
                     '0392.hk.zip','000063.sz.zip','ccl.ax^e21.zip','wipr.ns.zip',
                     'redy.ns.zip','0210.hk.zip','hclt.ns.zip','1128.hk.zip',
                     '0700.hk.zip','bbca.jk.zip','rals.jk.zip','pubm.kl.zip',
                     'unvr.jk.zip','036570.ks.zip','infy.ns.zip','600183.ss.zip',
                     'ioc.ns.zip','gamu.kl.zip','pshl.kl.zip','ammb.kl.zip',
                     'tena.kl.zip','urc.ps.zip','krib.kl.zip','005930.ks.zip',
                     'sbi.ns.zip','axia.kl.zip','african_alliance.zip','phdc.ca.zip',
                     'tmgh.ca.zip','phillip_securities_r.zip','phillipcapital_(indi.zip',
                     'iqcd.qa.zip','qisb.qa.zip','rbc_capital_markets.zip','cnr.to.zip',
                     'santander_gcb.zip','bz_wbk_s.a.zip','alrr.wa.zip','alep.wa.zip',
                     'sndk.oq^e16.zip','air.pa.zip','srenh.s.zip','002390.sz.zip',
                     'urka.mm.zip'
                     ]

    new_filenames = ['abg_sundal_collier.zip', 'aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip',
                     'aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip',
                     'aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip',
                     'aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip',
                     'aminvestment_bank.zip','aminvestment_bank.zip','aminvestment_bank.zip','auerbach_grayson__c.zip',
                     'auerbach_grayson__c.zip','auerbach_grayson__c.zip','auerbach_grayson__c.zip','auerbach_grayson__c.zip',
                     'auerbach_grayson__c.zip','auerbach_grayson__c.zip','auerbach_grayson__c.zip','auerbach_grayson__c.zip',
                     'auerbach_grayson__c.zip','auerbach_grayson__c.zip','auerbach_grayson__c.zip','baillieu_holst.zip',
                     'barclays.zip','barclays.zip','barclays.zip', 'barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'barclays.zip','barclays.zip','barclays.zip','barclays.zip',
                     'bear_stearns.zip','bear_stearns.zip','bear_stearns.zip','bear_stearns.zip',
                     'beltone_financial.zip','bice_inversiones.zip','bnp_paribas.zip','bnp_paribas.zip',
                     'bnp_paribas.zip','bofa_global_research.zip','bofa_global_research.zip','bofa_global_research.zip',
                     'bofa_global_research.zip','bofa_global_research.zip','bofa_global_research.zip','bofa_global_research.zip',
                     'bofa_global_research.zip','bofa_global_research.zip','bofa_global_research.zip','bofa_global_research.zip',
                     'bofa_global_research.zip','bofa_global_research.zip','bofa_global_research.zip','cgsi_research.zip',
                     'clsa.zip','clsa.zip','clsa.zip','clsa.zip',
                     'clsa.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'credit_suisse.zip','credit_suisse.zip','credit_suisse.zip','credit_suisse.zip',
                     'dbs_bank.zip','dbs_bank.zip','dbs_bank.zip','dbs_bank.zip',
                     'dbs_bank.zip','dbs_bank.zip','dbs_bank.zip','dbs_bank.zip',
                     'dbs_bank.zip','dbs_bank.zip','dbs_bank.zip','dragon_capital.zip',
                     'esn.zip','esn.zip','esn.zip','esn.zip',
                     'eurobank.zip','eurobank.zip','eurobank.zip','eurobank.zip',
                     'fisrt_global.zip','garanti_bbva.zip','gbm.zip','gbm.zip',
                     'gbm.zip','itau_bba.zip','itau_bba.zip','itau_bba.zip',
                     'itau_bba.zip','itau_bba.zip','itau_bba.zip','itau_bba.zip',
                     'itau_bba.zip','itau_bba.zip','itau_bba.zip','itau_bba.zip',
                     'itau_bba.zip','itau_bba.zip','jpmorgan.zip','jpmorgan.zip',
                     'jpmorgan.zip','jpmorgan.zip','jpmorgan.zip','jpmorgan.zip',
                     'jpmorgan.zip','jpmorgan.zip','jpmorgan.zip','jpmorgan.zip',
                     'jpmorgan.zip','jpmorgan.zip','jpmorgan.zip','jpmorgan.zip',
                     'jpmorgan.zip','jpmorgan.zip','jpmorgan.zip','jpmorgan.zip',
                     'jpmorgan.zip','jpmorgan.zip','jpmorgan.zip','jpmorgan.zip',
                     'jpmorgan.zip','jpmorgan.zip','jpmorgan.zip','jyske_bank.zip',
                     'kt_zmico.zip','kt_zmico.zip','macquarie.zip','macquarie.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','nomura.zip','nomura.zip',
                     'nomura.zip','nomura.zip','pharos_research.zip','pharos_research.zip',
                     'pharos_research.zip','phillip_capital.zip','phillip_capital.zip',
                     'qnb.zip','qnb.zip','rbc.zip','rbc.zip',
                     'santander.zip','santander.zip','santander.zip','santander.zip',
                     'ubs_equities.zip','ubs_equities.zip','ubs_equities.zip','ubs_equities.zip','uralsib.zip']

    return rename_map, old_filenames, new_filenames

# %% ===========================================================================
# Run main function
# ==============================================================================

if __name__ == "__main__":
    main()



package com.oldtan.neu.star.toone

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class RichSourceFromMoreSQL extends RichSourceFunction[List[Map[String, Any]]] {

  var isRUNNING: Boolean = true
  var conn: Option[Connection] = None

  @throws("Due to the connect error then exit!")
  def getConnection: Option[Connection] = {
    //val DB_URL = "jdbc:oracle:thin:@10.101.37.65:1521:orcl19c"
    val DB_URL = """jdbc:oracle:thin:@(DESCRIPTION =(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.22.248.135)(PORT=1521))(LOAD_BALANCE=yes))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=jkorcl)))"""
    val USER = "ehr"
    val PASS = "neusoft"
    Class forName "oracle.jdbc.driver.OracleDriver"
    Option(DriverManager.getConnection(DB_URL, USER, PASS))
  }

  override def open(parameters: Configuration) = {
    super.open(parameters)
    conn = getConnection
  }

  override def cancel() = {
    isRUNNING = false
  }

  override def close() = {
    conn.foreach(_ close)
  }

  override def run(sourceContext: SourceFunction.SourceContext[List[Map[String, Any]]]) = {
    var pullDataTime = LocalDateTime.now.minusHours(2).minusDays(1)
    val psFun = (con: Connection) => {
      con prepareStatement """select
                                  a.organization_code AS ORG_CODE  ,-- VARCHAR2(18)  Y  医疗机构编码
                                 a.organization_name AS ORG_NAME  ,-- VARCHAR2(80)  Y  医疗机构名称
                                 a.Ws01_00_004_01 AS EPISODE_SUMMARY_ID  ,-- VARCHAR2(36)  Y  病案首页ID
                                 a.WS01_00_905_01 AS EPI_SUMM_TYPE_CODE  ,-- VARCHAR2(1)  Y  病案首页类型编码
                                 a.WS01_00_905_01 AS EPI_SUMM_TYPE_NAME  ,-- VARCHAR2(10)  Y  病案首页类型名称
                                 '' AS HEALTH_CARD_NO  ,-- VARCHAR2(36)    居民健康卡号
                                 a.local_id AS PATIENT_ID  ,-- VARCHAR2(36)  Y  患者ID号
                                 a.WS01_00_014_01 AS INPATIENT_VISIT_NO  ,-- VARCHAR2(36)  Y  住院流水号
                                 a.Ws01_00_004_01 AS MEDICAL_RECORD_NO  ,-- VARCHAR2(36)  Y  病案号
                                 a.WS02_10_090_01 AS ADMIT_TIMES  ,-- NUMBER(3)  Y  住院次数
                                 '' AS EXPENSE_SOURCE_CODE  ,-- VARCHAR2(2)  Y  医疗付费方式编码
                                 '' AS EXPENSE_SOURCE_NAME  ,-- VARCHAR2(24)  Y  医疗付费方式名称
                                 a.WS02_01_039_001 AS PERSONAL_NAME  ,-- VARCHAR2(40)  Y  姓名
                                 a.WS02_01_040_01 AS SEX_CODE  ,-- VARCHAR2(1)  Y  性别编码
                                 a.CT02_01_040_01 AS SEX_NAME  ,-- VARCHAR2(16)  Y  性别名称
                                 a.WS02_01_005_01_01 AS BIRTH_DATE  ,-- DATE(8)  Y  出生日期
                                 a.WS02_01_026_01 AS AGE_YEAR  ,-- VARCHAR2(3)    年龄(岁)
                                 '' AS AGE_MONTH  ,-- VARCHAR2(8)    年龄(月)
                                 a.WS02_01_031_01 AS IDENTITY_TYPE_CODE  ,-- VARCHAR2(2)  Y  身份证件类型编码
                                 a.CT02_01_031_01 AS IDENTITY_TYPE_NAME  ,-- VARCHAR2(24)  Y  身份证件类型名称
                                 a.WS02_01_030_01 AS IDENTITY_NO  ,-- VARCHAR2(24)  Y  身份证件号码
                                 '' AS ID_UNK_REASON_CODE  ,-- VARCHAR2(1)    证件号码不详具体原因编码
                                 '' AS ID_UNK_REASON_NAME  ,-- VARCHAR2(10)    证件号码不详具体原因名称
                                 '' AS OTH_REASON_DESC  ,-- VARCHAR2(40)    证件号码不详其他原因说明
                                 a.WS02_01_015_01 AS NATIONALITY_CODE  ,-- VARCHAR2(3)  Y  国籍编码
                                 a.CT02_01_015_01 AS NATIONALITY_NAME  ,-- VARCHAR2(40)  Y  国籍名称
                                 a.WS04_10_019_02 AS NB_BIRTH_WEIGHT1  ,-- NUMBER(5)    新生儿1出生体重
                                 '' AS NB_BIRTH_WEIGHT2  ,-- NUMBER(5)    新生儿2出生体重
                                 a.WS04_10_019_03 AS NB_ADMIT_WEIGHT  ,-- NUMBER(5)    新生儿入院体重
                                 a.WS02_01_009_01_04 AS BIRTH_ADDR_PROV  ,-- VARCHAR2(20)  Y  出生地-省份（直辖市、自治区）
                                 a.WS02_01_009_02_04 AS BIRTH_ADDR_CITY  ,-- VARCHAR2(24)  Y  出生地-市（地区、州）
                                 a.WS02_01_009_03_04 AS BIRTH_ADDR_COUNTY  ,-- VARCHAR2(32)  Y  出生地-县（区）
                                 a.WS02_01_009_01_01 AS NATIVE_PROV  ,-- VARCHAR2(20)    籍贯-省份（直辖市、自治区）
                                 a.WS02_01_009_02_01 AS NATIVE_CITY  ,-- VARCHAR2(24)    籍贯-市（地区、州）
                                 a.WS02_01_025_01 AS ETHIC_GROUP_CODE  ,-- VARCHAR2(2)  Y  民族编码
                                 a.CT02_01_025_01 AS ETHIC_GROUP_NAME  ,-- VARCHAR2(20)  Y  民族名称
                                 a.WS02_01_018_01 AS MARITAL_STATUS_CODE  ,-- VARCHAR2(2)  Y  婚姻状况编码
                                 a.CT02_01_018_01 AS MARITAL_STATUS_NAME  ,-- VARCHAR2(20)  Y  婚姻状况名称
                                 a.WS02_01_052_01 AS EMPLOY_STATUS_CODE  ,-- VARCHAR2(2)  Y  从业状况分类编码
                                 a.CT02_01_052_01 AS EMPLOY_STATUS_NAME  ,-- VARCHAR2(20)  Y  从业状况类别名称
                                 '' AS RESIDENCE_AREA_CODE  ,-- VARCHAR2(12)    现住地的行政区划编码
                                 a.WS02_01_901_06 AS RESIDENCE_ADDR  ,-- VARCHAR2(160)  Y  现住址
                                 a.WS02_01_009_01_06 AS RESIDENCE_PROV  ,-- VARCHAR2(20)    现住址-省份（直辖市、自治区）
                                 a.WS02_01_009_02_06 AS RESIDENCE_CITY  ,-- VARCHAR2(24)    现住址-市（地区、州）
                                 a.WS02_01_009_03_06 AS RESIDENCE_COUNTY  ,-- VARCHAR2(32)    现住址-县（区）
                                 a.WS02_01_009_04_06 AS RESIDENCE_TOWN  ,-- VARCHAR2(30)    现住址-乡（镇、街道办事处）
                                 a.WS02_01_009_05_06 AS RESIDENCE_VILL  ,-- VARCHAR2(30)    现住址-村（街、路、弄）
                                 a.WS02_01_009_06_06 AS RESIDENCE_HOUSE_NO  ,-- VARCHAR2(30)    现住址-门牌号
                                 a.WS02_01_047_03 AS RESIDENCE_POSTCODE  ,-- VARCHAR2(6)    现住址邮政编码
                                 a.WS02_01_010_01 AS PERSONAL_PHONE  ,-- VARCHAR2(16)  Y  本人联系电话
                                 '' AS INHABITANT_TYPE_CODE  ,-- VARCHAR2(2)  Y  常住类型编码
                                 '' AS INHABITANT_TYPE_NAME  ,-- VARCHAR2(20)  Y  常住类型名称
                                 '' AS REGIST_AREA_CODE  ,-- VARCHAR2(12)    户籍地的行政区划编码
                                 a.WS02_01_009_02_05 AS REGIST_ADDR_CITY  ,-- VARCHAR2(24)    户籍地址-市（地区、州）
                                 a.WS02_01_009_03_05 AS REGIST_ADDR_COUNTY  ,-- VARCHAR2(32)    户籍地址-县（区）
                                 a.WS02_01_009_05_05 AS REGIST_ADDR_VILL  ,-- VARCHAR2(30)    户籍地址-村（街、路、弄）
                                 a.WS02_01_009_04_05 AS REGIST_ADDR_TOWN  ,-- VARCHAR2(30)    户籍地址-乡（镇、街道办事处）
                                 a.WS02_01_009_06_05 AS REGIST_ADDR_HOUSE_NO  ,-- VARCHAR2(30)    户籍地址-门牌号
                                 a.WS02_01_047_04 AS REGIST_ADDR_POSTCODE  ,-- VARCHAR2(6)    户籍地址邮政编码
                                 a.WS08_10_007_01 AS EMPLOYER_NAME  ,-- VARCHAR2(80)    单位名称
                                 a.WS02_01_901_08 AS EMPLOYER_ADDR  ,-- VARCHAR2(160)    单位地址
                                 '' AS EMPLOYER_AREA_CODE  ,-- VARCHAR2(6)    单位地址行政区划编码
                                 a.WS02_01_009_05_10 AS EMPLOYER_ADDR_VILL  ,-- VARCHAR2(30)    单位地址-村（街、路、弄）
                                 a.WS02_01_009_01_10 AS EMPLOYER_ADDR_PROV  ,-- VARCHAR2(20)    单位地址-省份（直辖市、自治区）
                                 a.WS02_01_009_02_10 AS EMPLOYER_ADDR_CITY  ,-- VARCHAR2(24)    单位地址-市（地区、州）
                                 a.WS02_01_009_04_10 AS EMPLOYER_ADDR_TOWN  ,-- VARCHAR2(30)    单位地址-乡（镇、街道办事处）
                                 a.WS02_01_009_03_10 AS EMPLOYER_ADDR_COUNTY  ,-- VARCHAR2(32)    单位地址-县（区）
                                 '' AS EMPLOYER_HOUSE_NO  ,-- VARCHAR2(30)    单位地址-门牌号
                                 '' AS EMPLOYER_POSTCODE  ,-- VARCHAR2(6)    单位地址邮政编码
                                 a.WS02_01_010_14 AS EMPLOYER_TEL_NO  ,-- VARCHAR2(16)    单位电话号码
                                 a.WS02_01_901_13 AS CONTACT_ADDR  ,-- VARCHAR2(160)  Y  联系人地址
                                 a.WS02_01_039_011 AS CONTACT_NAME  ,-- VARCHAR2(40)  Y  联系人姓名
                                 a.WS02_01_010_02 AS CONTACT_TEL  ,-- VARCHAR2(16)  Y  联系人电话
                                 a.WS02_10_024_02 AS CONTACT_REL_CODE  ,-- VARCHAR2(2)    联系人关系编码
                                 a.CT02_10_024_02 AS CONTACT_REL_NAME  ,-- VARCHAR2(40)    联系人关系名称
                                 a.CT06_00_339_01 AS PATIENT_SOURCE_NAME  ,-- VARCHAR2(10)  Y  入院途径名称
                                 a.WS06_00_339_01 AS PATIENT_SOURCE_CODE  ,-- VARCHAR2(1)  Y  入院途径编码
                                 '' AS MEDICINE_TYPE_CODE  ,-- VARCHAR2(2)  Y  治疗（医学）类别编码
                                 '' AS MEDICINE_TYPE_NAME  ,-- VARCHAR2(10)  Y  治疗（医学）类别名称
                                 a.WS06_00_092_01 AS ADMIT_DTIME  ,-- DATE(14)  Y  入院日期时间
                                 a.WS08_10_025_07 AS ADMIT_WARD_CODE  ,-- VARCHAR2(20)    入院病区编码
                                 a.CT08_10_025_07 AS ADMIT_WARD_NAME  ,-- VARCHAR2(40)  Y  入院病区名称
                                 a.WS08_10_025_07 AS ADMIT_DEPT_CODE  ,-- VARCHAR2(20)  Y  入院科室编码
                                 a.CT08_10_025_07 AS ADMIT_DEPT_NAME  ,-- VARCHAR2(40)  Y  入院科室名称
                                 a.WS08_10_025_07 AS ADMIT_DEPT_CODE_STD  ,-- VARCHAR2(6)  Y  入院科室编码（标准化）
                                 a.CT08_10_025_07 AS ADMIT_DEPT_NAME_STD  ,-- VARCHAR2(40)  Y  入院科室名称（标准化）
                                 '' AS TRANSFER_DEPT_CODE  ,-- VARCHAR2(200)    转科科别编码
                                 '' AS TRANSFER_DEPT_DESC  ,-- VARCHAR2(200)    转科科别描述
                                 a.WS06_00_017_01 AS DISCHARGE_DTIME  ,-- DATE(14)  Y  出院日期时间
                                 a.WS08_10_025_13 AS DISCHARGE_DEPT_CODE  ,-- VARCHAR2(20)  Y  出院科室编码
                                 a.CT08_10_025_13 AS DISCHARGE_DEPT_NAME  ,-- VARCHAR2(40)  Y  出院科室名称
                                 a.WS08_10_025_13 AS DISCH_DEPT_NAME_STD  ,-- VARCHAR2(40)  Y  出院科室名称（标准化）
                                 a.CT08_10_025_13 AS DISCH_DEPT_CODE_STD  ,-- VARCHAR2(6)  Y  出院科室编码（标准化）
                                 a.WS08_10_025_13 AS DISCHARGE_WARD_CODE  ,-- VARCHAR2(20)    出院病区编码
                                 a.CT08_10_025_13 AS DISCHARGE_WARD_NAME  ,-- VARCHAR2(40)  Y  出院病区名称
                                 a.WS06_00_310_01 AS LENGTH_OF_STAY  ,-- NUMBER(5)  Y  住院天数
                                 a.WS05_01_024_33 AS WM_OPT_DIAG_CODE_RAW  ,-- VARCHAR2(20)  Y  门急诊西医诊断编码（原始值）
                                 a.CT05_01_024_33 AS WM_OPT_DIAG_NAME_RAW  ,-- VARCHAR2(100)  Y  门急诊西医诊断名称（原始值）
                                 a.WS05_01_024_33 AS WM_OPT_DIAG_CODE  ,-- VARCHAR2(20)    门急诊西医诊断编码
                                 a.CT05_01_024_33 AS WM_OPT_DIAG_NAME  ,-- VARCHAR2(100)    门急诊西医诊断名称
                                 '' AS CM_OPT_DIAG_NAME_RAW  ,-- VARCHAR2(100)    门急诊中医诊断名称（原始值）
                                 '' AS CM_OPT_DIAG_CODE_RAW  ,-- VARCHAR2(20)    门急诊中医诊断编码（原始值）
                                 '' AS CM_OPT_DIAG_CODE  ,-- VARCHAR2(10)    门急诊中医诊断编码
                                 '' AS CM_OPT_DIAG_NAME  ,-- VARCHAR2(40)    门急诊中医诊断名称
                                 '' AS DRG_CODE  ,-- VARCHAR2(20)    DRGs分组编码
                                 '' AS DRG_NAME  ,-- VARCHAR2(100)    DRGs分组名称
                                 '' AS CM_PATHWAY_TYPE_CODE  ,-- VARCHAR2(1)    实施临床路径类别编码
                                 '' AS CM_PATHWAY_TYPE_NAME  ,-- VARCHAR2(10)    实施临床路径类别名称
                                 a.WS06_00_243_01 AS CM_HOS_PREP_MARK  ,-- VARCHAR2(1)    使用医疗机构中药制剂标记
                                 a.WS06_00_244_01 AS CM_EQUIP_MARK  ,-- VARCHAR2(1)    使用中医诊疗设备标记
                                 a.WS06_00_245_01 AS CM_TECH_MARK  ,-- VARCHAR2(1)    使用中医诊疗技术标记
                                 a.WS06_00_180_01 AS CM_DIALEC_NUR_MARK  ,-- VARCHAR2(1)    辨证施护标记
                                 '' AS EXT_CAUSE_NAME  ,-- VARCHAR2(100)    损伤、中毒的外部原因名称
                                 '' AS EXT_CAUSE_CODE  ,-- VARCHAR2(20)    损伤、中毒的外部原因编码
                                 '' AS EXT_CAUSE_CODE_RAW  ,-- VARCHAR2(20)    损伤、中毒的外部原因编码（原始值）
                                 '' AS EXT_CAUSE_NAME_RAW  ,-- VARCHAR2(100)    损伤、中毒的外部原因名称（原始值）
                                 '' AS PATHO_DIAG_NAME_RAW  ,-- VARCHAR2(100)    病理诊断名称（原始值）
                                 '' AS PATHO_DIAG_CODE_RAW  ,-- VARCHAR2(20)    病理诊断编码（原始值）
                                 '' AS PATHO_DIAG_NAME  ,-- VARCHAR2(100)    病理诊断名称
                                 '' AS PATHO_DIAG_CODE  ,-- VARCHAR2(20)    病理诊断编码
                                 '' AS PATHO_EXAM_NO  ,-- VARCHAR2(36)    病理号
                                 '' AS DIFFEREN_GRADE_CODE  ,-- VARCHAR2(1)    分化程度编码
                                 '' AS DIFFEREN_GRADE_NAME  ,-- VARCHAR2(20)    分化程度名称
                                 case when CT05_01_022_02 is not null then '是' else '否' end AS DRUG_ALLERGY_MARK  ,-- VARCHAR2(1)    药物过敏标记
                                 a.CT05_01_022_02 AS ALLERGY_DRUGS  ,-- VARCHAR2(100)    过敏药物名称
                                 a.WS09_00_108_01 AS AUTOPSY_MARK  ,-- VARCHAR2(1)    死亡患者尸检标记
                                 a.WS04_50_001_01 AS ABO_TYPE_CODE  ,-- VARCHAR2(1)    ABO血型编码
                                 a.CT04_50_001_01 AS ABO_TYPE_NAME  ,-- VARCHAR2(10)    ABO血型名称
                                 a.WS04_50_010_01 AS RH_TYPE_CODE  ,-- VARCHAR2(1)    Rh血型编码
                                 a.CT04_50_010_01 AS RH_TYPE_NAME  ,-- VARCHAR2(10)    Rh血型名称
                                 a.WS02_01_039_034 AS DEPT_DIRECTOR_NAME  ,-- VARCHAR2(40)  Y  科主任姓名
                                 '' AS DEPT_DIRECTOR_ID  ,-- VARCHAR2(18)    科主任工号
                                 '' AS CHIEF_PHYSICIAN_ID  ,-- VARCHAR2(18)    （副）主任医师工号
                                 a.WS02_01_039_033 AS CHIEF_PHYSICIAN_NAME  ,-- VARCHAR2(40)  Y  （副）主任医师姓名
                                 '' AS ATTENDING_ID  ,-- VARCHAR2(18)    主治医师工号
                                 a.WS02_01_039_030 AS ATTENDING_NAME  ,-- VARCHAR2(40)  Y  主治医师姓名
                                 '' AS RESIDENT_DOC_ID  ,-- VARCHAR2(18)    住院医师工号
                                 a.WS02_01_039_029 AS RESIDENT_DOC_NAME  ,-- VARCHAR2(40)  Y  住院医师姓名
                                 '' AS PRIMARY_NURSE_ID  ,-- VARCHAR2(18)    责任护士工号
                                 a.WS02_01_039_067 AS PRIMARY_NURSE_NAME  ,-- VARCHAR2(40)  Y  责任护士姓名
                                 '' AS REFRESHER_ID  ,-- VARCHAR2(18)    进修医师工号
                                 a.WS02_01_039_047 AS REFRESHER_NAME  ,-- VARCHAR2(40)    进修医师姓名
                                 '' AS INTERN_ID  ,-- VARCHAR2(18)    实习医师工号
                                 a.WS02_01_039_059 AS INTERN_NAME  ,-- VARCHAR2(40)    实习医师姓名
                                 '' AS CODE_ENTERER_ID  ,-- VARCHAR2(18)    编码员工号
                                 a.WS02_01_039_086 AS CODE_ENTERER_NAME  ,-- VARCHAR2(40)    编码员姓名
                                 a.WS09_00_103_01 AS MR_QUALITY_CODE  ,-- VARCHAR2(1)  Y  病案质量等级编码
                                 a.CT09_00_103_01 AS MR_QUALITY_NAME  ,-- VARCHAR2(4)  Y  病案质量等级名称
                                 '' AS QC_DOCTOR_ID  ,-- VARCHAR2(18)    质控医生工号
                                 a.WS02_01_039_053 AS QC_DOCTOR_NAME  ,-- VARCHAR2(40)  Y  质控医生姓名
                                 '' AS QC_NURSE_ID  ,-- VARCHAR2(18)    质控护士工号
                                 a.WS02_01_039_052 AS QC_NURSE_NAME  ,-- VARCHAR2(40)  Y  质控护士姓名
                                 a.WS09_00_120_01 AS QC_DATE  ,-- DATE(8)  Y  质控日期
                                 '' AS IS_SURG_PROCEDURE  ,-- VARCHAR2(1)    手术操作标记
                                 '' AS SURG_PROCEDURE_NUM  ,-- NUMBER(2)    手术操作记录数量
                                 a.WS06_00_223_01 AS DISCHARGE_WAY_CODE  ,-- VARCHAR2(1)  Y  离院方式编码
                                 a.CT06_00_223_01 AS DISCHARGE_WAY_NAME  ,-- VARCHAR2(40)  Y  离院方式名称
                                 a.CT08_10_052_06 AS DESTINATION_ORG_NAME  ,-- VARCHAR2(80)    转往机构名称
                                 a.WS06_00_194_01 AS READMIT_PLAN_31DAY  ,-- VARCHAR2(1)  Y  是否有出院31天内再住院计划
                                 a.WS06_00_195_01 AS READMIT_PURPOSE  ,-- VARCHAR2(100)    再住院计划目的
                                 '' AS PRE_ADM_COMA_DAYS  ,-- NUMBER(5)    入院前昏迷天数
                                 '' AS PRE_ADM_COMA_HOURS  ,-- NUMBER(2)    入院前昏迷小时数
                                 '' AS PRE_ADM_COME_MINS  ,-- NUMBER(2)    入院前昏迷分钟数
                                 '' AS POST_ADM_COMA_DAYS  ,-- NUMBER(5)    入院后昏迷天数
                                 '' AS POST_ADM_COMA_HOURS  ,-- NUMBER(2)    入院后昏迷小时数
                                 '' AS POST_ADM_COME_MINS  ,-- NUMBER(2)    入院后昏迷分钟数
                                 '' AS TOTAL_EXPENSE  ,-- NUMBER(10)  Y  医疗总费用
                                 '' AS OUT_OF_POCKET  ,-- NUMBER(10)  Y  自付金额
                                 b.GEN_MED_SERV_FEE AS GEN_MED_SERV_FEE  ,-- NUMBER(10)  Y  一般医疗服务费
                                 b.CM_DIALEC_ANALY_FEE AS CM_DIALEC_ANALY_FEE  ,-- NUMBER(10)  Y  其中：中医辩证论治费
                                 b.CM_DIALEC_CONSUL_FEE AS CM_DIALEC_CONSUL_FEE  ,-- NUMBER(10)  Y  其中：中医辩证论治会诊费
                                 b.GEN_TRATMENT_FEE AS GEN_TRATMENT_FEE  ,-- NUMBER(10)  Y  一般治疗操作费
                                 b.NURSING_FEE AS NURSING_FEE  ,-- NUMBER(10)  Y  护理费
                                 b.OTH_GEN_SERV_FEE AS OTH_GEN_SERV_FEE  ,-- NUMBER(10)  Y  其他医疗服务费用
                                 b.PATHO_DIAG_FEE AS PATHO_DIAG_FEE  ,-- NUMBER(10)  Y  病理诊断费
                                 LAB_DIAG_FEE AS LAB_DIAG_FEE  ,-- NUMBER(10)  Y  实验室诊断费
                                 CLIN_DIAG_FEE AS CLIN_DIAG_FEE  ,-- NUMBER(10)  Y  临床诊断项目费
                                 IMAG_DIAG_FEE AS IMAG_DIAG_FEE  ,-- NUMBER(10)  Y  影像学诊断费
                                 NON_SURG_TREAT_FEE AS NON_SURG_TREAT_FEE  ,-- NUMBER(10)  Y  非手术治疗项目费
                                 PHYSIOTHERAPY_FEE AS PHYSIOTHERAPY_FEE  ,-- NUMBER(10)  Y  其中：临床物理治疗费
                                 SURG_TREAT_FEE AS SURG_TREAT_FEE  ,-- NUMBER(10)  Y  手术治疗费
                                 ANESTHESIA_FEE AS ANESTHESIA_FEE  ,-- NUMBER(10)  Y  其中：麻醉费
                                 SURGERY_FEE AS SURGERY_FEE  ,-- NUMBER(10)  Y  其中：手术费
                                 REHAB_FEE AS REHAB_FEE  ,-- NUMBER(10)  Y  康复费
                                 CM_GIAG_FEE AS CM_GIAG_FEE  ,-- NUMBER(10)  Y  中医诊断费
                                 TCM_TREAT_FEE AS TCM_TREAT_FEE  ,-- NUMBER(10)  Y  中医治疗费
                                 CM_EXT_THERA_FEE AS CM_EXT_THERA_FEE  ,-- NUMBER(10)  Y  其中：中医外治
                                 CM_OSTO_TRAUM_FEE AS CM_OSTO_TRAUM_FEE  ,-- NUMBER(10)  Y  其中：中医骨伤
                                 CM_ANCUP_MOXIB_FEE AS CM_ANCUP_MOXIB_FEE  ,-- NUMBER(10)  Y  其中：针刺及灸法
                                 CM_TUINA_FEE AS CM_TUINA_FEE  ,-- NUMBER(10)  Y  其中：中医推拿治疗
                                 CM_PROCTO_FEE AS CM_PROCTO_FEE  ,-- NUMBER(10)  Y  其中：中医肛肠治疗
                                 CM_SPECIAL_TREAT_FEE AS CM_SPECIAL_TREAT_FEE  ,-- NUMBER(10)  Y  其中：中医特殊治疗
                                 CM_OTH_TCM_FEE AS CM_OTH_TCM_FEE  ,-- NUMBER(10)  Y  中医其他费
                                 CM_FORMULA_PROC_FEE AS CM_FORMULA_PROC_FEE  ,-- NUMBER(10)  Y  其中：中医特殊配方加工
                                 CM_DIALECT_DIET_FEE AS CM_DIALECT_DIET_FEE  ,-- NUMBER(10)  Y  其中：辩证施善
                                 W_MEDICINE_FEE AS W_MEDICINE_FEE  ,-- NUMBER(10)  Y  西药费
                                 ANTIBIOTICS_FEE AS ANTIBIOTICS_FEE  ,-- NUMBER(10)  Y  其中：抗菌药物费用
                                 PATENT_TCM_FEE AS PATENT_TCM_FEE  ,-- NUMBER(10)  Y  中成药费
                                 CM_HOS_PREP_FEE AS CM_HOS_PREP_FEE  ,-- NUMBER(10)  Y  其中：中药制剂费
                                 HERB_FEE AS HERB_FEE  ,-- NUMBER(10)  Y  中草药费
                                 BLOOD_FEE AS BLOOD_FEE  ,-- NUMBER(10)  Y  血费
                                 ALBUMIN_FEE AS ALBUMIN_FEE  ,-- NUMBER(10)  Y  白蛋白类制品费
                                 GLOBULIN_FEE AS GLOBULIN_FEE  ,-- NUMBER(10)  Y  球蛋白类制品费
                                 COAGULAT_FACTOR_FEE AS COAGULAT_FACTOR_FEE  ,-- NUMBER(10)  Y  凝血因子类制品费
                                 CYTOKINE_FEE AS CYTOKINE_FEE  ,-- NUMBER(10)  Y  细胞因子类制品费
                                 EXAM_MATERIAL_FEE AS EXAM_MATERIAL_FEE  ,-- NUMBER(10)  Y  检查用一次性医用材料费
                                 TREAT_MATERIAL_FEE AS TREAT_MATERIAL_FEE  ,-- NUMBER(10)  Y  治疗用一次性医用材料费
                                 SURGERY_MATERIAL_FEE AS SURGERY_MATERIAL_FEE  ,-- NUMBER(10)  Y  手术用一次性医用材料费
                                 OTHER_FEE AS OTHER_FEE  ,-- NUMBER(10)  Y  其他费用
                                 a.WS06_00_913_01 AS CREATE_DTIME  ,-- DATE(14)  Y  创建时间
                                 a.WS09_00_120_01 AS ARCHIVE_DTIME  ,-- DATE(14)  Y  归档日期时间
                                 '归档' AS RECORD_STATUS_CODE  ,-- VARCHAR2(1)  Y  病历/档案记录状态
                                 'N' AS CONFIDENTIALITY_CODE  ,-- VARCHAR2(1)    保密级别
                                 a.datagenerate_date AS LAST_UPDATE_DTIME,-- DATE(14)  Y  最后更新时间
                                 '' AS RESOURCE_ID  -- VARCHAR2(36)    资源信息ID
                             
                             from hai_aprnot_info a left join (select business_id,
                                                             datagenerate_date,
                                                             organization_code,
                                                                 GEN_MED_SERV_FEE,
                                                             PATENT_TCM_FEE,
                                                             GEN_TRATMENT_FEE,
                                                             NURSING_FEE,
                                                             OTH_GEN_SERV_FEE,
                                                             PATHO_DIAG_FEE,
                                                             LAB_DIAG_FEE,
                                                             IMAG_DIAG_FEE,
                                                             CLIN_DIAG_FEE,
                                                             NON_SURG_TREAT_FEE,
                                                             SURG_TREAT_FEE,
                                                             REHAB_FEE,
                                                             CM_GIAG_FEE,
                                                             TCM_TREAT_FEE,
                                                             CM_OTH_TCM_FEE,
                                                             W_MEDICINE_FEE,
                                                             HERB_FEE,
                                                             BLOOD_FEE,
                                                             ALBUMIN_FEE,
                                                             GLOBULIN_FEE,
                                                             COAGULAT_FACTOR_FEE,
                                                             CYTOKINE_FEE,
                                                             EXAM_MATERIAL_FEE,
                                                             TREAT_MATERIAL_FEE,
                                                             SURGERY_MATERIAL_FEE,
                                                             OTHER_FEE,
                                                             CM_DIALEC_ANALY_FEE,
                                                             CM_DIALEC_CONSUL_FEE,
                                                             PHYSIOTHERAPY_FEE,
                                                             ANESTHESIA_FEE,
                                                             SURGERY_FEE,
                                                             CM_EXT_THERA_FEE,
                                                             CM_OSTO_TRAUM_FEE,
                                                             CM_ANCUP_MOXIB_FEE,
                                                             CM_TUINA_FEE,
                                                             CM_PROCTO_FEE,
                                                             CM_SPECIAL_TREAT_FEE,
                                                             CM_FORMULA_PROC_FEE,
                                                             CM_DIALECT_DIET_FEE,
                                                             ANTIBIOTICS_FEE,
                                                             CM_HOS_PREP_FEE
                                                      from
                                                          (select t.business_id, t.ct07_00_907_01, t.ws07_00_908_01,
                                                                  t.datagenerate_date, t.domain_code, t.organization_code, t.organization_name
                                                           from hai_expens_info t)
                                                              pivot (sum(ws07_00_908_01) for ct07_00_907_01 in (
                                                              '一般医疗服务费' as GEN_MED_SERV_FEE,
                                                              '一般治疗操作费' as GEN_TRATMENT_FEE,
                                                              '护理费' as NURSING_FEE,
                                                              '其他' as OTH_GEN_SERV_FEE,
                                                              '病理诊断费' as PATHO_DIAG_FEE,
                                                              '实验室诊断费' as LAB_DIAG_FEE,
                                                              '影像学诊断费' as IMAG_DIAG_FEE,
                                                              '临床诊断项目费' as CLIN_DIAG_FEE,
                                                              '非手术治疗项目费' as NON_SURG_TREAT_FEE,
                                                              '手术治疗费' as SURG_TREAT_FEE,
                                                              '康复费' as REHAB_FEE,
                                                              '中医诊断' as CM_GIAG_FEE,
                                                              '中医治疗' as TCM_TREAT_FEE,
                                                              '中医其他' as CM_OTH_TCM_FEE,
                                                              '西药费' as W_MEDICINE_FEE,
                                                              '中成药费' as PATENT_TCM_FEE,
                                                              '中草药费' as HERB_FEE,
                                                              '血费' as BLOOD_FEE,
                                                              '白蛋白类制品费' as ALBUMIN_FEE,
                                                              '球蛋白类制品费' as GLOBULIN_FEE,
                                                              '凝血因子类制品费' as COAGULAT_FACTOR_FEE,
                                                              '细胞因子类制品费' as CYTOKINE_FEE,
                                                              '检查用一次性医用材料费' as EXAM_MATERIAL_FEE,
                                                              '治疗用一次性医用材料费' as TREAT_MATERIAL_FEE,
                                                              '手术用一次性医用材料费' as SURGERY_MATERIAL_FEE,
                                                              '其他费' as OTHER_FEE,
                                                              '中医辩证论治费' as CM_DIALEC_ANALY_FEE,
                                                              '中医辩证论治会诊费' as CM_DIALEC_CONSUL_FEE,
                                                              '临床物理治疗费' as PHYSIOTHERAPY_FEE,
                                                              '麻醉费' as ANESTHESIA_FEE,
                                                              '手术费' as SURGERY_FEE,
                                                              '中医外治' as CM_EXT_THERA_FEE,
                                                              '中医骨伤' as CM_OSTO_TRAUM_FEE,
                                                              '针刺与灸法' as CM_ANCUP_MOXIB_FEE,
                                                              '中医推拿治疗' as CM_TUINA_FEE,
                                                              '中医肛肠治疗' as CM_PROCTO_FEE,
                                                              '中医特殊治疗' as CM_SPECIAL_TREAT_FEE,
                                                              '中药特殊调配加工' as CM_FORMULA_PROC_FEE,
                                                              '辩证施膳' as CM_DIALECT_DIET_FEE,
                                                              '抗菌药物费用' as ANTIBIOTICS_FEE,
                                                              '医疗机构中药制剂费' as CM_HOS_PREP_FEE
                                                              )
                                                              )
                             ) b
                               on a.business_id = b.business_id and a.organization_code = b.organization_code
                               where a.datagenerate_date >= ? and a.datagenerate_date < ?"""
    }
    val ps = psFun(conn.get)
    while (true) if (pullDataTime.plusHours(2).isBefore(LocalDateTime.now)){
      ps.setString(1, DateTimeFormatter.ofPattern("yyyyMMddHH%").format(pullDataTime))
      ps.setString(2, DateTimeFormatter.ofPattern("yyyyMMddHH%").format(pullDataTime.plusHours(1)))
      val resSet = ps.executeQuery
      var resultData:ListBuffer[Map[String, Any]] = mutable.ListBuffer.empty
      new Iterator[ResultSet] {
        def hasNext = resSet next
        def next = resSet
      }.toStream
        .foreach(r => {
          if(resultData.length >= 2000){
            sourceContext collect resultData.toList
            resultData clear
          }
          /**
            *  yichangtest tantest
            *  1、Included _meta key and key is string result data sink to Rest api
            *  2、Not included _meta key and key is int result data sink to Oracle
            */
          //resultData += (Map("_meta" -> Map("dataset" -> "yichangtest", "rowkey" -> UUID.randomUUID.toString)) ++
          //  (1 to r.getMetaData.getColumnCount).toIterator.map(i => (r.getMetaData.getColumnName(i), r getString i)).toMap)
          resultData += (1 to r.getMetaData.getColumnCount).toIterator.map(i => (String.valueOf(i), r getString i)).toMap
        })
      if (resultData.nonEmpty) sourceContext collect resultData.toList
      println(pullDataTime)
      pullDataTime = pullDataTime plusHours 1
      Thread sleep 60*60*1000
    }
    ps close
  }
}

@echo off
REM ============================================================================
REM 每月利润报表数据同步脚本 (Windows版本)
REM 功能：按顺序执行利润报表数据采集、计算字段更新、费用单创建
REM 更新范围：从上个月1号到今天
REM 使用方法：每月手动执行一次
REM ============================================================================

chcp 65001 >nul
setlocal enabledelayedexpansion

REM 获取项目根目录
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
cd /d "%PROJECT_ROOT%"

REM 颜色代码（Windows 10+）
set "COLOR_INFO=[94m"
set "COLOR_SUCCESS=[92m"
set "COLOR_WARNING=[93m"
set "COLOR_ERROR=[91m"
set "COLOR_RESET=[0m"

echo.
echo %COLOR_INFO%============================================%COLOR_RESET%
echo %COLOR_INFO%每月利润报表数据同步开始%COLOR_RESET%
echo %COLOR_INFO%============================================%COLOR_RESET%
echo.

REM 记录开始时间
set START_TIME=%TIME%

REM 计算日期范围（上个月1号到今天）
for /f "tokens=1-3 delims=/ " %%a in ('date /t') do (
    set CURRENT_YEAR=%%a
    set CURRENT_MONTH=%%b
    set CURRENT_DAY=%%c
)

REM 去除月份前导零
set /a CURRENT_MONTH_NUM=10%CURRENT_MONTH% %% 100

REM 计算上个月
set /a LAST_MONTH_NUM=%CURRENT_MONTH_NUM%-1
if %LAST_MONTH_NUM% LSS 1 (
    set /a LAST_MONTH_NUM=12
    set /a LAST_YEAR=%CURRENT_YEAR%-1
) else (
    set LAST_YEAR=%CURRENT_YEAR%
)

REM 格式化月份（补零）
if %LAST_MONTH_NUM% LSS 10 (
    set LAST_MONTH=0%LAST_MONTH_NUM%
) else (
    set LAST_MONTH=%LAST_MONTH_NUM%
)

REM 设置日期范围
set START_DATE=%LAST_YEAR%-%LAST_MONTH%-01
set END_DATE=%CURRENT_YEAR%-%CURRENT_MONTH%-%CURRENT_DAY%

echo %COLOR_INFO%[INFO]%COLOR_RESET% 计算日期范围: %START_DATE% ~ %END_DATE%
echo.

REM 检查Python环境
echo %COLOR_INFO%[INFO]%COLOR_RESET% 检查Python环境...

REM 检查虚拟环境
if exist "venv\Scripts\activate.bat" (
    echo %COLOR_INFO%[INFO]%COLOR_RESET% 发现虚拟环境，正在激活...
    call venv\Scripts\activate.bat
) else if exist ".venv\Scripts\activate.bat" (
    echo %COLOR_INFO%[INFO]%COLOR_RESET% 发现虚拟环境，正在激活...
    call .venv\Scripts\activate.bat
) else (
    echo %COLOR_WARNING%[WARNING]%COLOR_RESET% 未发现虚拟环境，使用系统Python
)

REM 检查Python版本
python --version
if errorlevel 1 (
    echo %COLOR_ERROR%[ERROR]%COLOR_RESET% Python未安装或不在PATH中
    exit /b 1
)
echo.

REM ============================================================================
REM 步骤1: 采集利润报表数据
REM ============================================================================
echo %COLOR_INFO%==========================================%COLOR_RESET%
echo %COLOR_INFO%步骤 1/3: 采集利润报表数据（从领星API）%COLOR_RESET%
echo %COLOR_INFO%==========================================%COLOR_RESET%
echo.

python jobs\Sync_data\fetch_profit_report_msku_daily.py --start-date %START_DATE% --end-date %END_DATE%
if errorlevel 1 (
    echo.
    echo %COLOR_ERROR%[ERROR]%COLOR_RESET% 利润报表数据采集失败，终止执行
    exit /b 1
)

echo.
echo %COLOR_SUCCESS%[SUCCESS]%COLOR_RESET% 利润报表数据采集完成！
echo %COLOR_INFO%[INFO]%COLOR_RESET% 等待3秒...
timeout /t 3 /nobreak >nul
echo.

REM ============================================================================
REM 步骤2: 更新利润报表计算字段
REM ============================================================================
echo %COLOR_INFO%==========================================%COLOR_RESET%
echo %COLOR_INFO%步骤 2/3: 更新利润报表计算字段（头程单价、单品毛重等）%COLOR_RESET%
echo %COLOR_INFO%==========================================%COLOR_RESET%
echo.

python jobs\Sync_data\update_profit_report_calculated_fields.py --start-date %START_DATE% --end-date %END_DATE%
if errorlevel 1 (
    echo.
    echo %COLOR_ERROR%[ERROR]%COLOR_RESET% 利润报表计算字段更新失败，终止执行
    exit /b 1
)

echo.
echo %COLOR_SUCCESS%[SUCCESS]%COLOR_RESET% 利润报表计算字段更新完成！
echo %COLOR_INFO%[INFO]%COLOR_RESET% 等待3秒...
timeout /t 3 /nobreak >nul
echo.

REM ============================================================================
REM 步骤3: 创建费用单
REM ============================================================================
echo %COLOR_INFO%==========================================%COLOR_RESET%
echo %COLOR_INFO%步骤 3/3: 创建费用单（根据利润报表数据）%COLOR_RESET%
echo %COLOR_INFO%==========================================%COLOR_RESET%
echo.

python jobs\Sync_data\create_fee_management.py --start-date %START_DATE% --end-date %END_DATE% --daily
if errorlevel 1 (
    echo.
    echo %COLOR_ERROR%[ERROR]%COLOR_RESET% 费用单创建失败，终止执行
    exit /b 1
)

echo.
echo %COLOR_SUCCESS%[SUCCESS]%COLOR_RESET% 费用单创建完成！
echo.

REM 计算总耗时
set END_TIME=%TIME%
echo %COLOR_INFO%============================================%COLOR_RESET%
echo %COLOR_SUCCESS%所有任务执行完成！%COLOR_RESET%
echo %COLOR_INFO%日期范围: %START_DATE% ~ %END_DATE%%COLOR_RESET%
echo %COLOR_INFO%开始时间: %START_TIME%%COLOR_RESET%
echo %COLOR_INFO%结束时间: %END_TIME%%COLOR_RESET%
echo %COLOR_INFO%============================================%COLOR_RESET%
echo.

pause





/* -----------------------------------
* Replication Ball and Brown PBFJ 2019
* ---------------------------------- */

* Import data
use ../data/adata.dta, clear

* Check unique ids
unique gvkey datadate

* Select eps var
gen eps = epspi

* Filter: fyear
keep if inrange(fyear, 1971, 2019)

* Compute 1-yr change in eps
bys gvkey (datadate): gen deps = eps - eps[_n-1] if fyear - fyear[_n-1] == 1 & fyr == fyr[_n-1]

* Compute reporting lag
gen lag = dofc(rdq) - dofc(datadate)

* Compute 1-yr change in lag
bys gvkey (datadate): gen dlag = lag - lag[_n-1] if fyear - fyear[_n-1] == 1 & fyr == fyr[_n-1]

* Indicator variable for good vs. bad earnings news
gen g = .
replace g = 1 if deps > 0
replace g = 0 if deps < 0

* Days between t0 and t1
gen tau = ( dofc(t1) - dofc(t0) )
tab tau


/* ----------------
* Descriptive stats
* ---------------- */

* Filter I: non-missing contemporaneous EPS
keep if eps != .

/* Descriptive statistics of reporting lags - Tab 1A */ {
preserve

	local N = 49
	tabstat lag, by(fyear) col(stat) stat(N p50 p1 p25 p75 p99) nototal save
	tabstatmat T

	svmat T
	keep T*
	keep if T1~=.

	local x = 1 
	foreach var in N p50 p1 p25 p75 p99 {
		 rename T`x' `var'
		 local x = `x' + 1
	}

	gen year = 1971
	forval x = 2/`N'{
		replace year = 1970 + `x' if _n == `x'
	}
	sort year

	order year N 

	foreach var in year N p50 p1 p25 p75 p99 {
		tostring `var', replace force
	}

	export delimited using ../LaTeX_Outputs/t1a.csv, replace
	
restore
}

* Filter II: two consecutive EPS reports
keep if deps != . & (dofc(t0) - dofc(rdq)) <= 5

/* Descriptive statistics of reporting lags - Tab 1B */ {
preserve

	local N = 48
	tabstat dlag, by(fyear) col(stat) stat(N p50) nototal save
	tabstatmat T

	svmat T
	keep T*
	keep if T1~=.

	local x = 1 
	foreach var in N p50 {
		 rename T`x' `var'
		 local x = `x' + 1
	}

	gen year = 1972
	forval x = 2/`N'{
		replace year = 1971 + `x' if _n == `x'
	}
	sort year

	order year N 

	foreach var in year N p50 {
		tostring `var', replace force
	}

	export delimited using ../LaTeX_Outputs/t1b.csv, replace
	
restore
}

* Temp data: event-view earnings annoucement data
duplicates drop permno t0 t1, force
save ../temp/adata.dta, replace


/* ------------
* Main Analysis
* ------------ */

* Import event-day data
use ../data/wdata.dta, clear

* Check unique ids
unique permno t0 t1 date

* Cumulative returns
bys permno t0 (date): gen cr = sum( log(1 + ret) )
bys permno t0 (date): gen car = sum( log(1 + ret -  ewretd) ) // approx.

* Assign event-level characteristics: good vs. bad news, fyear, etc.
merge m:1 permno t0 t1 using ../temp/adata.dta, keepusing(permno t0 t1 tau g gvkey datadate fyear)

* Compute event day, t
gen t = ( dofc(date) - dofc(t1) )

* Filter III: non-missing good/bad news indicator & event day 
keep if g != . & t != .

/* Event-window cum. ab. ret. - Fig 1A */ {
preserve
	
	* Mean cum. ret.
	collapse (mean) car, by(g t)
	reshape wide car, i(t) j(g)
	
	* Demean by averages across the two ports
	egen avgcar = rowmean(car0 car1)
	replace car1 = car1 - avgcar
	replace car0 = car0 - avgcar
	
	* Plotting
	twoway line car1 car0 t, xtitle("") ytitle("CAR") ///
		lp(solid longdash) lw(1 1) legend(label(1 "Good news") label(2 "Bad news")) ///
		graphregion(color(white)) ylab(,nogrid) xlabel(-360(90)180) ylabel(-.15(.05).15) ///
		xline(`=0', lpat(dash) lc(black)) yline(`=0', lpat(dot) lc(black))
	graph export ../LaTeX_Outputs/1a.pdf, replace
	
restore
}

/* Event-window cum. raw ret. - Fig 1B */ {
preserve
	
	* Mean cum. ret.
	collapse (mean) cr, by(g t)
	reshape wide cr, i(t) j(g)
	
	* Plotting
	twoway line cr1 cr0 t, xtitle("") ytitle("Cum. Ret.") ///
		lp(solid longdash) lw(1 1) legend(label(1 "Good news") label(2 "Bad news")) ///
		graphregion(color(white)) ylab(,nogrid) xlabel(-360(90)180) ylabel(-.15(.05).15) ///
		xline(`=0', lpat(dash) lc(black)) yline(`=0', lpat(dot) lc(black))
	graph export ../LaTeX_Outputs/1b.pdf, replace
	
restore
}

* Sub-period indicator: pre-event, event-day, post-event
gen prd = .
replace prd = 1 if inrange(t, -360, -tau-1)
replace prd = 2 if inrange(t, -tau, 0)
replace prd = 3 if inrange(t, 1, 180)

* Compute sub-period cum. ret.
bys permno t0 prd (date): gen cr_prd = sum( log(1 + ret) )
bys permno t0 prd (date): gen car_prd = sum( log(1 + ret -  ewretd) ) // approx.

bys permno t0 prd (t): keep if _n == _N 

/* Output temp data for arallel processing outside of STATA */
keep permno t0 fyear g prd cr_prd car_prd
save ../temp/prd.dta, replace

/* By-year by-subperiod good vs. bad news - Tab 2A */ {
preserve
	
	* Count of good vs. bad news
	collapse (count) car_prd, by(fyear prd g)
	reshape wide car_prd, i(fyear prd) j(g)
	
	* Duplicates drop
	drop prd
	duplicates drop
	
	* Output to csv
	export delimited using ../LaTeX_Outputs/t2a.csv, replace
	
restore
}

/* By-year by-subperiod cum. ret. - Tab 2B */ {
preserve
	
	* Mean cum. ab. ret. 
	collapse (mean) car_prd, by(fyear prd g)
	reshape wide car_prd, i(fyear prd) j(g)
	
	* Compute separation (group diff)
	gen d_car_prd = car_prd1 - car_prd0
	drop car_prd*
	reshape wide d_car_prd, i(fyear) j(prd)
	
	* Formatting
	tostring d_car_prd*, force replace format(%7.3fc)
	
	* Output to csv
	export delimited using ../LaTeX_Outputs/t2b.csv, replace
	
restore
}
	
	
	
	






























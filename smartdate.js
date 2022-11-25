module.exports = class SmartDate {
 constructor(dateStr) {
     if (dateStr) {
        this.dt = new Date(dateStr);
        return;     
     }
    this.dt = new Date();
 }
 //add day and reset time 
 addDayResetTime(days) {
    this.dt.setDate(this.dt.getDate() + days);
    return this;
 }
 
 firstMonthDay() {
   this.dt.setDate(1);
   return this;
 }

 lastMonthDay() {
   this.firstMonthDay().addMonth(1).addDay(-1);
   return this;
 }
 
addHours(hours) {
   this.dt.setTime(this.dt.getTime() + hours * 3600 * 1000);
   return this;
}
addMinutes(mins) {
   this.dt.setTime(this.dt.getTime() + mins * 60 * 1000);
   return this;
}
//add day and save time 
addDay(days) {
    this.dt.setTime(this.dt.getTime() + days * 24 * 3600 * 1000);
    return this;
 }
 addMonth(mon) {
    this.dt.setMonth(this.dt.getMonth() + mon);
    return this;
 }
 nextHour() {
    this.dt.setHours(this.dt.getHours() + 1);
    this.dt.setMinutes(0, 0, 0); // Resets also seconds and milliseconds
  return this;
 }
 roundMinutes() {
    this.dt.setHours(this.dt.getHours() + Math.round(this.dt.getMinutes()/60));
    this.dt.setMinutes(0, 0, 0); // Resets also seconds and milliseconds
  return this;
 }
 resetMinutes() {
   this.dt.setMinutes(0, 0, 0); // Resets also seconds and milliseconds
 return this;
}

currGasDay() {
    this.dt.setHours(this.dt.getHours() -7);
    this.dt.setHours(7);
    this.dt.setMinutes(0, 0, 0); // Resets also seconds and milliseconds
  return this;
 }

 nextGasDay() {
    this.currGasDay().addDay(1);
  return this;
 }

 lastGasDay() {
    this.currGasDay().addDay(-1);
  return this;
 }

}
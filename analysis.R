
### Get P values
ps = c()
b1s = c()
ses = c()
dofs = c()
for(i in 1:60){
  filename = paste('hypothesis/h',i,'.csv',sep='')
  d = read.csv(filename)
  l = lm(ys~xs,data = d)
  se = coef(summary(l))['xs','Std. Error']
  b1 = l$coefficients['xs']
  tstat = b1 / se
  dof = length(d) - 2
  quantile = pt(tstat,dof)
  pvalue = 1 - abs(quantile - .5)*2
#  print(summary(l))
  ps = c(ps,pvalue)
  b1s = c(b1s,b1)
  ses = c(ses,se)
  dofs = c(dofs,dof)
}

### Run Benjamini Hochberg Procedure

alpha = .1
n = length(ps)
kmax = 0
for(k in 1:n){
  if(sum(ps < alpha * k / n) >= k){
    kmax = k
  }
}
res = which(ps < alpha * kmax / n)

### Selective Inference
R = length(res)
lbs = c()
rbs = c()
for(r in res){
  lb = qt((alpha*kmax/2/n), dofs[r])
  rb = qt(1 - (alpha*kmax/2/n),dofs[r])
  lb = ses[r] * lb + b1s[r]
  rb = ses[r] * rb + b1s[r]
  lbs = c(lbs,lb)
  rbs = c(rbs,rb)
}
plot(res,b1s[res],ylim=c(0,2),xlab ='hypothesis number ',ylab = 'beta1')

for(i in 1:kmax){
  segments(res[i],rbs[i],y1=lbs[i])
}

sigmas = 1/b1s
siglbs = 1/rbs
sigrbs = 1/lbs
plot(res,sigmas[res],ylim = c(0,3),xlab= 'hypothesis number',ylab = 'elasticity of substitution')
for(i in 1:kmax){
  segments(res[i],siglbs[i],y1=sigrbs[i])
}


from flask import Flask, render_template, request, redirect
import profile_manager, crawler, cv_generator, ats_optimizer

app = Flask(__name__)

@app.route("/")
def dashboard():
    return render_template("dashboard.html")

@app.route("/profile", methods=["GET","POST"])
def profile():
    if request.method == "POST":
        profile_manager.save_user_profile(request.form)
        return redirect("/")
    profile = profile_manager.load_user_profile()
    return render_template("profile.html", profile=profile)

@app.route("/skills", methods=["GET","POST"])
def skills():
    if request.method == "POST":
        skills = request.form["skills"].split(",")
        profile_manager.save_skills(skills)
        return redirect("/")
    skills = profile_manager.load_skills()
    return render_template("skills.html", skills=skills)

@app.route("/experience", methods=["GET","POST"])
def experience():
    if request.method == "POST":
        # Expecting multiple rows
        profile_manager.save_experience(request.form)
        return redirect("/")
    experience = profile_manager.load_experience()
    return render_template("experience.html", experience=experience)

@app.route("/jobs")
def jobs():
    jobs = crawler.load_jobs()
    return render_template("jobs.html", jobs=jobs)

@app.route("/company_jobs", methods=["GET","POST"])
def company_jobs():
    if request.method == "POST":
        companies = request.form["companies"].split(",")
        keywords = request.form["keywords"].split(",")
        jobs = crawler.crawl_company_careers(companies, keywords)
        return render_template("company_jobs.html", jobs=jobs)
    jobs = crawler.load_company_jobs()
    return render_template("company_jobs.html", jobs=jobs)

@app.route("/generate_cv", methods=["GET","POST"])
def generate_cv():
    if request.method == "POST":
        job_id = request.form.get("job_id")
        if job_id:
            ats_optimizer.generate_ats_cv(int(job_id))
        else:
            cv_generator.generate_cv()
        return redirect("/")
    return render_template("generate_cv.html")
